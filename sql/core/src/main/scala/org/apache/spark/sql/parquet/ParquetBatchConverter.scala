/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.parquet

import java.sql.Timestamp
import java.util.{TimeZone, Calendar}

import scala.collection.mutable.{Buffer, ArrayBuffer, HashMap}

import jodd.datetime.JDateTime
import parquet.column.Dictionary
import parquet.io.api.{PrimitiveConverter, GroupConverter, Binary, Converter}
import parquet.schema.MessageType

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.parquet.CatalystConverter.FieldType
import org.apache.spark.sql.types._
import org.apache.spark.sql.parquet.timestamp.NanoTime
/**
 * A `parquet.io.api.GroupConverter` that is able to convert a Parquet record
 * to a [[org.apache.spark.sql.catalyst.expressions.Row]] object. Note that his
 * converter is optimized for rows of primitive types (non-nested records).
 */
private[parquet] object CatalystBatchConverter {
  def createBatchConverter(
      field: FieldType,
      fieldIndex: Int): CatalystBatchPrimitiveConverter = {
    val fieldType: DataType = field.dataType
    fieldType match {
      case dt: BooleanType =>
        new CatalystBatchBooleanConverter(dt, fieldIndex)
      case dt: ByteType =>
        new CatalystBatchByteConverter(dt, fieldIndex)
      case dt: ShortType =>
        new CatalystBatchShortConverter(dt, fieldIndex)
      case dt: IntegerType =>
        new CatalystBatchIntConverter(dt, fieldIndex)
      case dt: LongType =>
        new CatalystBatchLongConverter(dt, fieldIndex)
      case dt: FloatType =>
        new CatalystBatchFloatConverter(dt, fieldIndex)
      case dt: DoubleType =>
        new CatalystBatchDoubleConverter(dt, fieldIndex)
      case dt: BinaryType =>
        new CatalystBatchBinaryConverter(dt, fieldIndex)
      case dt: StringType =>
        new CatalystBatchStringConverter(dt, fieldIndex)
      case dt: DecimalType =>
        new CatalystBatchDecimalConverter(dt, fieldIndex)
      case _ => throw new RuntimeException(
        s"unable to convert datatype ${field.dataType.toString} in CatalystConverter")
    }
  }
}
  
private[parquet] class CatalystBatchPrimitiveRowConverter(
    protected[parquet] val schema: Array[FieldType],
    protected[parquet] val converters: Array[CatalystBatchPrimitiveConverter],
    protected[parquet] val batch: RowBatch)
  extends CatalystConverter {
  def this(schema: Array[FieldType], converters: Array[CatalystBatchPrimitiveConverter]) = {
    this(schema, converters, new RowBatch(schema, converters))
  }
  def this(schema: Array[FieldType]) = {
    this(schema, schema.zipWithIndex.map {
      case (field, idx) => CatalystBatchConverter.createBatchConverter(field, idx)
    })
  }
  def this(attributes: Array[Attribute]) = {
    this(attributes.map(a => new FieldType(a.name, a.dataType, a.nullable)))
  }

  override val size = schema.size

  override val index = 0

  override val parent = null

  // Should be only called in root group converter!
  override def getCurrentRecord: Row = batch.curRow

  override def skipCurrentRecord: Unit = batch.skipCurRow

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  // for child converters to update upstream values
  override protected[parquet] def updateField(fieldIndex: Int, value: Any): Unit = {
    throw new UnsupportedOperationException // child converters should use the
    // specific batch methods
  }

  override protected[parquet] def clearBuffer(): Unit = {}

  override def start(): Unit = {}

  override def startBatch(): Unit = batch.start()
  override def endBatch(): Unit = batch.end()
  override def end(): Unit = {}
}

class RowBatch(schema: Array[FieldType], val converters: Array[CatalystBatchPrimitiveConverter]) {
  val numCols = schema.length
  val maxNumRows = GroupConverter.ROW_BATCH_SIZE
  private[this] var numRows = 0
  private[this] var readIndex = 0
  def start() = {
    readIndex = 0
    numRows = 0
  }

  def end() = {
    var i = 0
    var firstNonZeroNumRows = 0
    while (i < converters.length) {
      val c = converters(i).numRows
      if (firstNonZeroNumRows == 0) {
        firstNonZeroNumRows = c
      } else {
        // take into account partition columns that will have only a row or so
        if (c > 0 && c != firstNonZeroNumRows) {
          val lengths = converters.map{_.numRows}.deep
          sys.error(s"each column in a batch doesn't have same number of values: ${lengths}")
        }
      }
      i += 1
    }
    numRows = firstNonZeroNumRows
  }

  private[this] def newValueArray() = new Array[Any](schema.length)
  private[this] val row = new RowBatchRow(this, new StructType(schema), newValueArray())

  def curRow = {
    if (readIndex >= numRows) sys.error(s"reading beyond the end of batch at index ${readIndex}")
    row.setCursorAt(readIndex)
    readIndex += 1
    row
  }

  def skipCurRow = {
    if (readIndex >= numRows) sys.error(s"skipping beyond the end of batch at index ${readIndex}")
    readIndex += 1
  }
}

final class RowBatchRow(
   val batch: RowBatch,
   override val schema: StructType,
   val values: Array[Any],
   private[this] var valuesCopied:Boolean = false) extends MutableRow {
  private[this] var _index = 0
  def setCursorAt(i: Int) = { _index = i; valuesCopied = false }
  def setNullAt(i: Int): Unit = if (batch != null) batch.converters(i).setNullValue(_index)
    else values.update(i, null)

  def update(i: Int, value: Any): Unit =
    if (batch != null) batch.converters(i).update(_index, value) else values.update(i, value)
  override def setInt(i: Int, value: Int) = if (batch == null) values.update(i, value)
    else batch.converters(i).setInt(_index, value)
  override def setLong(i: Int, value: Long) = if (batch == null) values.update(i, value)
    else batch.converters(i).setLong(_index, value)
  override def setDouble(i: Int, value: Double) = if (batch == null) values.update(i, value)
    else batch.converters(i).setDouble(_index, value)
  override def setBoolean(i: Int, value: Boolean) = if (batch == null) values.update(i, value)
    else batch.converters(i).setBoolean(_index, value)
  override def setShort(i: Int, value: Short) = if (batch == null) values.update(i, value)
    else batch.converters(i).setShort(_index, value)
  override def setByte(i: Int, value: Byte) = if (batch == null) values.update(i, value)
    else batch.converters(i).setByte(_index, value)
  override def setFloat(i: Int, value: Float) = if (batch == null) values.update(i, value)
    else batch.converters(i).setFloat(_index, value)
  override def setString(i: Int, value: String) = if (batch == null) values.update(i, value)
    else batch.converters(i).setString(_index, value)

  override def apply(i: Int): Any = if (batch != null) batch.converters(i)(_index)
    else values(i)
  override def toSeq: Seq[Any] = if (batch != null) copyValues()
    else values
  override def length = values.length
  override def isNullAt(i: Int): Boolean =
    if (batch != null) batch.converters(i).isNullValue(_index) else values(i) == null
  override def getInt(i: Int): Int =
    if (batch != null) batch.converters(i).getInt(_index)
    else values(i).asInstanceOf[Int]
  override def getLong(i: Int): Long =
    if (batch != null) batch.converters(i).getLong(_index)
    else values(i).asInstanceOf[Long]
  override def getDouble(i: Int): Double =
    if (batch != null) batch.converters(i).getDouble(_index)
    else values(i).asInstanceOf[Double]
  override def getFloat(i: Int): Float =
    if (batch != null) batch.converters(i).getFloat(_index)
    else values(i).asInstanceOf[Float]
  override def getBoolean(i: Int): Boolean =
    if (batch != null) batch.converters(i).getBoolean(_index)
    else values(i).asInstanceOf[Boolean]
  override def getShort(i: Int): Short =
    if (batch != null) batch.converters(i).getShort(_index)
    else values(i).asInstanceOf[Short]
  override def getByte(i: Int): Byte =
    if (batch != null) batch.converters(i).getByte(_index)
    else values(i).asInstanceOf[Byte]
  override def getString(i: Int): String =
    if (batch != null) batch.converters(i).getString(_index)
    else values(i).asInstanceOf[String]
  override def getAs[T](i: Int): T =
    if (batch != null) batch.converters(i)(_index).asInstanceOf[T]
    else values(i).asInstanceOf[T]

  private[parquet] def copyValues(): Array[Any] = {
    if (!valuesCopied) {
      var i = 0
      while (i < values.length) {
        val converter = batch.converters(i)
        if (!converter.isNullValue(_index)) values.update(i, converter(_index))
        else values.update(i, null)
        i += 1
      }
      valuesCopied = true
    }
    values
  }

  def copy(): GenericRow = {
    val newValues = if (batch != null) {
      val src = copyValues()
      val dest = new Array[Any](src.length)
      System.arraycopy(src, 0, dest, 0, src.length)
      dest
     }
    else values
    new GenericRow(newValues)
  }
}

/**
 * A `parquet.io.api.PrimitiveConverter` that converts Parquet types to Catalyst types
 * in a batch mode. This converter is used when a Row consists of only primitive types.
 * Hence, there is no parent converter to which a call may be delegated to.
 * 
 * @param batch The store where a batch of values are stored for the field
 * @param fieldIndex The index inside the record.
 */
private[parquet] abstract class CatalystBatchPrimitiveConverter(
    fieldType: DataType,
    fieldIndex: Int) extends PrimitiveConverter {
  override def hasBatchSupport: Boolean = true
  val maxNumRows = GroupConverter.ROW_BATCH_SIZE
  private[this] var _numRows = 0
  private[this] var readIndex = 0
  val nullFlags = new Array[Boolean](maxNumRows)
  private[parquet] def numRows = _numRows
  private[parquet] def numRows_=(size: Int) = _numRows = size
  protected[this] def checkMaxSize(size: Int) = if (size > maxNumRows) {
    sys.error(s"Can't support batch size $size, only upto $maxNumRows supported")
  }
  override def getNullIndicatorBatchStore(maxSize: Int) = { checkMaxSize(maxSize); nullFlags }
  override def startOfBatchOp = {}
  override def endOfBatchOp(filledBatchSize: Int) = {
    readIndex = 0
    _numRows = filledBatchSize
  }

  /*
   * The following methods exist to support MutableRow semantics in a row returned
   * by CatalystBatchPrimitiveRowConverter, just like a row returned by
   * CatalystPrimitiveRowConverter supports these methods.
   */
  private[this] val unsupported = s"unsupported by ${getClass.getName}"
  private[parquet] def apply(i: Int): Any
  private[parquet] def update(i: Int, v: Any): Unit
  private[parquet] def isNullValue(i: Int) = nullFlags(i)
  private[parquet] def setNullValue(i: Int) = nullFlags(i) = true
  private[parquet] def getBoolean(i: Int): Boolean = sys.error(unsupported)
  private[parquet] def setBoolean(i: Int, v: Boolean): Unit = sys.error(unsupported)
  private[parquet] def getByte(i: Int): Byte = sys.error(unsupported)
  private[parquet] def setByte(i: Int, v: Byte): Unit = sys.error(unsupported)
  private[parquet] def getShort(i: Int): Short = sys.error(unsupported)
  private[parquet] def setShort(i: Int, v: Short): Unit = sys.error(unsupported)
  private[parquet] def getInt(i: Int): Int = sys.error(unsupported)
  private[parquet] def setInt(i: Int, v: Int): Unit = sys.error(unsupported)
  private[parquet] def getLong(i: Int): Long = sys.error(unsupported)
  private[parquet] def setLong(i: Int, v: Long): Unit = sys.error(unsupported)
  private[parquet] def getFloat(i: Int): Float = sys.error(unsupported)
  private[parquet] def setFloat(i: Int, v: Float): Unit = sys.error(unsupported)
  private[parquet] def getDouble(i: Int): Double = sys.error(unsupported)
  private[parquet] def setDouble(i: Int, v: Double): Unit = sys.error(unsupported)
  private[parquet] def getBinary(i: Int): Binary = sys.error(unsupported)
  private[parquet] def setBinary(i: Int, v: Binary): Unit = sys.error(unsupported)
  private[parquet] def getString(i: Int): String = sys.error(unsupported)
  private[parquet] def setString(i: Int, v: String): Unit = sys.error(unsupported)
}

private[parquet] class CatalystBatchBooleanConverter(
    fieldType: BooleanType,
    fieldIndex: Int) extends CatalystBatchPrimitiveConverter(fieldType, fieldIndex) {
  val booleanColVals = new Array[Boolean](maxNumRows)
  override def getBooleanBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize)
    booleanColVals
  }

  private[parquet] override def getBoolean(i: Int) = booleanColVals(i)
  private[parquet] override def setBoolean(i: Int, v: Boolean) = {
    nullFlags(i) = false
    booleanColVals(i) = v
  }

  override def apply(i: Int): Any = if (nullFlags(i)) null else booleanColVals(i)

  private[parquet] override def update(i: Int, v: Any): Unit = {
    // Used only for setting partition values.
    if (v == null) nullFlags(i) = true else {
      nullFlags(i) = false
      booleanColVals(i) = v.asInstanceOf[Boolean]
    }
  }
}

private[parquet] class CatalystBatchByteConverter(
    fieldType: ByteType,
    fieldIndex: Int) extends CatalystBatchPrimitiveConverter(fieldType, fieldIndex) {
  val intColVals = new Array[Int](maxNumRows)
  override def getIntBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize)
    intColVals
  }

  private[parquet] override def getByte(i: Int) = intColVals(i).asInstanceOf[Byte]
  private[parquet] override def setByte(i: Int, v: Byte) = {
    nullFlags(i) = false
    intColVals(i) = v
  }

  override def apply(i: Int): Any = if (nullFlags(i)) null else intColVals(i).toByte

  private[parquet] override def update(i: Int, v: Any): Unit = {
    // Used only for setting partition values.
    if (v == null) nullFlags(i) = true else {
      nullFlags(i) = false
      intColVals(i) = v.asInstanceOf[Byte].toInt
    }
  }
}

private[parquet] class CatalystBatchShortConverter(
    fieldType: ShortType,
    fieldIndex: Int) extends CatalystBatchPrimitiveConverter(fieldType, fieldIndex) {
  val intColVals = new Array[Int](maxNumRows)
  override def getIntBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize)
    intColVals
  }

  private[parquet] override def getShort(i: Int) = intColVals(i).asInstanceOf[Short]
  private[parquet] override def setShort(i: Int, v: Short) = {
    nullFlags(i) = false
    intColVals(i) = v
  }

  override def apply(i: Int): Any = if (nullFlags(i)) null else intColVals(i).toShort

  private[parquet] override def update(i: Int, v: Any): Unit = {
    // Used only for setting partition values.
    if (v == null) nullFlags(i) = true else {
      nullFlags(i) = false
      intColVals(i) = v.asInstanceOf[Short].toInt
    }
  }
}

private[parquet] class CatalystBatchIntConverter(
    fieldType: IntegerType,
    fieldIndex: Int) extends CatalystBatchPrimitiveConverter(fieldType, fieldIndex) {
  val intColVals = new Array[Int](maxNumRows)
  override def getIntBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize)
    intColVals
  }

  private[parquet] override def getInt(i: Int) = intColVals(i)
  private[parquet] override def setInt(i: Int, v: Int) = {
    nullFlags(i) = false
    intColVals(i) = v
  }

  override def apply(i: Int): Any = if (nullFlags(i)) null else intColVals(i)

  private[parquet] override def update(i: Int, v: Any): Unit = {
    // Used only for setting partition values.
    if (v == null) nullFlags(i) = true else {
      nullFlags(i) = false
      intColVals(i) = v.asInstanceOf[Int]
    }
  }
}

private[parquet] class CatalystBatchLongConverter(
    fieldType: LongType,
    fieldIndex: Int) extends CatalystBatchPrimitiveConverter(fieldType, fieldIndex) {
  val longColVals = new Array[Long](maxNumRows)
  override def getLongBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize)
    longColVals
  }

  private[parquet] override def getLong(i: Int) = longColVals(i)
  private[parquet] override def setLong(i: Int, v: Long) = {
    nullFlags(i) = false
    longColVals(i) = v
  }

  override def apply(i: Int): Any = if (nullFlags(i)) null else longColVals(i)

  private[parquet] override def update(i: Int, v: Any): Unit = {
    // Used only for setting partition values.
    if (v == null) nullFlags(i) = true else {
      nullFlags(i) = false
      longColVals(i) = v.asInstanceOf[Long]
    }
  }
}

private[parquet] class CatalystBatchFloatConverter(
    fieldType: FloatType,
    fieldIndex: Int) extends CatalystBatchPrimitiveConverter(fieldType, fieldIndex) {
  val floatColVals = new Array[Float](maxNumRows)
  override def getFloatBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize)
    floatColVals
  }

  private[parquet] override def getFloat(i: Int) = floatColVals(i)
  private[parquet] override def setFloat(i: Int, v: Float) = {
    nullFlags(i) = false
    floatColVals(i) = v
  }
  override def apply(i: Int): Any = if (nullFlags(i)) null else floatColVals(i)

  private[parquet] override def update(i: Int, v: Any): Unit = {
    // Used only for setting partition values.
    if (v == null) nullFlags(i) = true else {
      nullFlags(i) = false
      floatColVals(i) = v.asInstanceOf[Float]
    }
  }
}

private[parquet] class CatalystBatchDoubleConverter(
    fieldType: DoubleType,
    fieldIndex: Int) extends CatalystBatchPrimitiveConverter(fieldType, fieldIndex) {
  val doubleColVals = new Array[Double](maxNumRows)
  override def getDoubleBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize)
    doubleColVals
  }

  private[parquet] override def getDouble(i: Int) = doubleColVals(i)
  private[parquet] override def setDouble(i: Int, v: Double) = {
    nullFlags(i) = false
    doubleColVals(i) = v
  }

  override def apply(i: Int): Any = if (nullFlags(i)) null else doubleColVals(i)

  private[parquet] override def update(i: Int, v: Any): Unit = {
    // Used only for setting partition values.
    if (v == null) nullFlags(i) = true else {
      nullFlags(i) = false
      doubleColVals(i) = v.asInstanceOf[Double]
    }
  }
}

/**
 * A `parquet.io.api.PrimitiveConverter` that converts Parquet Binary to Array[Byte].
 * Supports dictionaries to reduce Binary to Array[Byte] conversion overhead.
 *
 * Follows pattern in Parquet of using dictionaries, where supported, for Array[Byte] conversion.
 *
 * @param fieldType The type of the field.
 * @param fieldIndex The index of the field inside the record.
 */
private[parquet] class CatalystBatchBinaryConverter(fieldType: BinaryType, fieldIndex: Int)
  extends CatalystBatchPrimitiveConverter(fieldType, fieldIndex) {

  private[this] var dict: Array[Array[Byte]] = null
  private[this] var dictLookupBatchStore: Array[Int] = null
  private[this] val colValsBinary = new Array[Binary](maxNumRows)
  private[this] val convertedVals = new Array[Array[Byte]](maxNumRows)
  override def getBinaryBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize)
    colValsBinary
  }

  private[this] def initConvertedVals() {
    var i = 0
    while (i < convertedVals.length) {
      convertedVals(i) = null
      i += 1
    }
  }
  override def hasDictionarySupport: Boolean = true

  override def setDictionary(dictionary: Dictionary):Unit = {
    dict = Array.tabulate(dictionary.getMaxId + 1) { dictionary.decodeToBinary(_).getBytes }
  }

  override def getDictLookupBatchStore(maxSize: Int) = {
    dictLookupBatchStore = new Array[Int](maxSize)
    dictLookupBatchStore
  }

  override def endOfDictBatchOp(filledBatchSize:Int): Unit = {
    var i = 0
    while (i < filledBatchSize) {
      if (!nullFlags(i)) convertedVals(i) = dict(dictLookupBatchStore(i))
      i += 1
    }
    numRows = filledBatchSize
  }

  override def endOfBatchOp(filledBatchSize:Int): Unit = {
    numRows = filledBatchSize
    initConvertedVals()
  }

  /*
   * The following methods exist to support MutableRow semantics in a row returned by
   * a CatalystPrimitiveRowConverter
   */
  private[parquet] override def getBinary(i: Int) = colValsBinary(i)
  private[parquet] override def setBinary(i: Int, v: Binary) = {
    if (v == null) nullFlags(i) = true else nullFlags(i) = false
    colValsBinary(i) = v
  }

  private[parquet] override def apply(i: Int): Any = {
    if (isNullValue(i)) null else {
      val bytes = convertedVals(i)
      if (bytes != null) bytes else {
        val bytes = colValsBinary(i).getBytes
        convertedVals(i) = bytes
        bytes
      }
    }
  }
  private[parquet] override def getString(i: Int): String = {
    if (nullFlags(i)) null else colValsBinary(i).toStringUsingUTF8
  }

  private[parquet] override def setString(i: Int, v: String) = {
    if (v == null) nullFlags(i) = true else {
      nullFlags(i) = false
      colValsBinary(i) = Binary.fromString(v)
      convertedVals(i) = null
    }
  }

  private[parquet] override def update(i: Int, v: Any): Unit = {
    // Assumption: Used only for setting partition values. So, the corresponding
    // binary values needn't be filled in.
    if (v == null) nullFlags(i) = true else {
      nullFlags(i) = false
      convertedVals(i) = v.asInstanceOf[Array[Byte]]
    }
  }
}

/**
 * A `parquet.io.api.PrimitiveConverter` that converts Parquet Binary to Catalyst String.
 * Supports dictionaries to reduce Binary to String conversion overhead.
 *
 * Follows pattern in Parquet of using dictionaries, where supported, for String conversion.
 *
 * @param fieldType The type of the field.
 * @param fieldIndex The index of the field inside the record.
 */
private[parquet] class CatalystBatchStringConverter(fieldType: StringType, fieldIndex: Int)
  extends CatalystBatchPrimitiveConverter(fieldType, fieldIndex) {

  private[this] var dict: Array[String] = null
  private[this] var dictLookupBatchStore: Array[Int] = null
  private[this] val colValsBinary = new Array[Binary](maxNumRows)
  private[this] val convertedVals = new Array[String](maxNumRows)
  override def getBinaryBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize)
    colValsBinary
  }

  private[this] def initConvertedVals() {
    var i = 0
    while (i < convertedVals.length) {
      convertedVals(i) = null
      i += 1
    }
  }
  override def hasDictionarySupport: Boolean = true

  override def setDictionary(dictionary: Dictionary):Unit = {
    dict = Array.tabulate(dictionary.getMaxId + 1) {
      dictionary.decodeToBinary(_).toStringUsingUTF8
    }
  }

  override def getDictLookupBatchStore(maxSize: Int) = {
    dictLookupBatchStore = new Array[Int](maxSize)
    dictLookupBatchStore
  }

  override def endOfDictBatchOp(filledBatchSize:Int): Unit = {
    var i = 0
    while (i < filledBatchSize) {
      if (!nullFlags(i)) convertedVals(i) = dict(dictLookupBatchStore(i))
      i += 1
    }
    numRows = filledBatchSize
  }

  override def endOfBatchOp(filledBatchSize:Int): Unit = {
    numRows = filledBatchSize
    initConvertedVals()
  }

  /*
   * The following methods exist to support MutableRow semantics in a row returned by
   * a CatalystPrimitiveRowConverter
   */
  private[parquet] override def apply(i: Int): Any = getString(i)
  private[parquet] override def getString(i: Int): String = {
    if (nullFlags(i)) return null
    val s = convertedVals(i)
    if (s != null) s else {
      val v = colValsBinary(i)
      val s = if (v != null) v.toStringUsingUTF8 else null
      convertedVals(i) = s
      s
    }
  }

  private[parquet] override def setString(i: Int, v: String) = {
    if (v == null) nullFlags(i) = true else {
      convertedVals(i) = v
      nullFlags(i) = false
    }
  }

  private[parquet] override def update(i: Int, v: Any): Unit = {
    // Assumption: Used only for setting partition values. So, the corresponding
    // storage type values needn't be filled in.
    if (v == null) nullFlags(i) = true else {
      nullFlags(i) = false
      convertedVals(i) = v.asInstanceOf[String]
    }
  }
}

/**
 * A `parquet.io.api.PrimitiveConverter` that converts Parquet Binary to Decimal.
 * Supports dictionaries to reduce Binary to Decimal conversion overhead.
 *
 * Follows pattern in Parquet of using dictionaries, where supported, for Decimal conversion.
 *
 * @param fieldType The type of the field.
 * @param fieldIndex The index of the field inside the record.
 */
private[parquet] class CatalystBatchDecimalConverter(fieldType: DecimalType, fieldIndex: Int)
  extends CatalystBatchPrimitiveConverter(fieldType, fieldIndex) {

  private[this] var dict: Array[Decimal] = null
  private[this] var dictLookupBatchStore: Array[Int] = null
  private[this] var dictInUse: Boolean = false
  private[this] val colValsBinary = new Array[Binary](maxNumRows)
  private[this] val convertedVals = Array.tabulate(maxNumRows) { i => new Decimal() }
  override def getBinaryBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize)
    colValsBinary
  }

  override def hasDictionarySupport: Boolean = true

  /**
   * Read a decimal value from a Parquet Binary into "dest". Only supports decimals that fit in
   * a long (i.e. precision <= 18)
   */
  private[this] def readDecimal(dest: Decimal, value: Binary, ctype: DecimalType): Decimal = {
    val precision = ctype.precisionInfo.get.precision
    val scale = ctype.precisionInfo.get.scale
    val bytes = value.getBytes
    require(bytes.length <= 16, "Decimal field too large to read")
    var unscaled = 0L
    var i = 0
    while (i < bytes.length) {
      unscaled = (unscaled << 8) | (bytes(i) & 0xFF)
      i += 1
    }
    // Make sure unscaled has the right sign, by sign-extending the first bit
    val numBits = 8 * bytes.length
    unscaled = (unscaled << (64 - numBits)) >> (64 - numBits)
    dest.setOrNull(unscaled, precision, scale)
  }

  override def setDictionary(dictionary: Dictionary): Unit = {
    // Need one extra slot to hold the partition value when dictionary
    // is in use. See update(i: Int, v: Any) implementation of this class.
    dict = Array.tabulate(dictionary.getMaxId + 2) { i => {
      val d = new Decimal()
      if (i <= dictionary.getMaxId) readDecimal(d, dictionary.decodeToBinary(i), fieldType)
      else d
    }}
  }

  override def getDictLookupBatchStore(maxSize: Int) = {
    dictLookupBatchStore = new Array[Int](maxSize)
    dictLookupBatchStore
  }

  override def endOfDictBatchOp(filledBatchSize:Int): Unit = {
    super.endOfBatchOp(filledBatchSize)
    dictInUse = true
  }

  override def endOfBatchOp(filledBatchSize:Int): Unit = {
    super.endOfBatchOp(filledBatchSize)
    dictInUse = false
  }

  private[this] def getDecimal(i: Int): BigDecimal = {
    if (isNullValue(i)) null else {
      if (!dictInUse) {
        val d = convertedVals(i)
        readDecimal(d, colValsBinary(i), fieldType)
        d.toBigDecimal
      } else {
        dict(dictLookupBatchStore(i)).toBigDecimal
      }
    }
  }

  /*
   * The following methods exist to support MutableRow semantics in a row returned by
   * a CatalystPrimitiveRowConverter
   */
  private[parquet] override def apply(i: Int): Any = getDecimal(i)
  private[parquet] override def getString(i: Int): String = {
    val d = getDecimal(i)
    if (d eq null) null else d.toString
  }

  private[parquet] override def update(i: Int, v: Any): Unit = {
    // Assumption: Used only for setting partition values. So, the corresponding
    // storage type values needn't be filled in.
    if (v == null) nullFlags(i) = true else {
      nullFlags(i) = false
      val bd = v.asInstanceOf[BigDecimal]
      if (dictInUse) {
        val d = dict(dict.length - 1)
        d.set(bd, fieldType.precision, fieldType.scale)
        dict(dict.length - 1) = d
        dictLookupBatchStore(i) = dict.length - 1
      } else {
        convertedVals(i).set(bd, fieldType.precision, fieldType.scale)
      }
    }
  }
}

import com.twitter.chill.Kryo
import com.esotericsoftware.kryo.Serializer

class RowBatchRowSerializer extends Serializer[RowBatchRow] {

  import com.twitter.chill.{ Input, Output }

  def write(kryo: Kryo, output: Output, obj: RowBatchRow) = {
    obj.copyValues() // ensure column values are copied to values array
    kryo.writeClassAndObject(output, obj.values.length)
    var i = 0
    while (i < obj.values.length) {
      kryo.writeClassAndObject(output, obj.values(i))
      i += 1
    }
  }

  def read(kryo: Kryo, input: Input, cls: Class[RowBatchRow]): RowBatchRow = {
    val len = kryo.readClassAndObject(input).asInstanceOf[Int]
    val values = new Array[Any](len)
    var i = 0
    while (i < len) {
      values(i) = kryo.readClassAndObject(input)
      i += 1
    }
    new RowBatchRow(null, null, values, true)
  }
}

