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
      case BinaryType =>
        new CatalystBatchBinaryConverter(fieldType, fieldIndex)
      case StringType =>
        new CatalystBatchStringConverter(fieldType, fieldIndex)
      // All other primitive types use the default converter
      case ctype: PrimitiveType => { // note: need the type tag here!
        new CatalystBatchPrimitiveConverter(fieldType, fieldIndex)
      }
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
    numRows = converters(0).numRows
    if (!converters.forall(_.numRows == numRows)) {
      val lengths = converters.map{_.numRows}.deep
      sys.error(s"each column in a batch doesn't have same number of values: ${lengths}")
    }
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
    else batch.converters(i).setInt(_index, value)
  override def setByte(i: Int, value: Byte) = if (batch == null) values.update(i, value)
    else batch.converters(i).setInt(_index, value)
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
    if (batch != null) batch.converters(i).getInt(_index).asInstanceOf[Short]
    else values(i).asInstanceOf[Short]
  override def getByte(i: Int): Byte =
    if (batch != null) batch.converters(i).getInt(_index).asInstanceOf[Byte]
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
private[parquet] class CatalystBatchPrimitiveConverter(
    fieldType: DataType,
    fieldIndex: Int) extends PrimitiveConverter {
  override def hasBatchSupport: Boolean = true
  val maxNumRows = GroupConverter.ROW_BATCH_SIZE
  private[this] var _numRows = 0
  private[this] var readIndex = 0
  val nullFlags = new Array[Boolean](maxNumRows)
  val colVals: Array[_] =  fieldType match {
      case BooleanType => Array.ofDim[Boolean](maxNumRows)
      case ByteType => Array.ofDim[Int](maxNumRows)
      case ShortType => Array.ofDim[Int](maxNumRows)
      case IntegerType => Array.ofDim[Int](maxNumRows)
      case LongType => Array.ofDim[Long](maxNumRows)
      case FloatType => Array.ofDim[Float](maxNumRows)
      case DoubleType => Array.ofDim[Double](maxNumRows)
      case StringType => Array.ofDim[Binary](maxNumRows)
      case BinaryType => Array.ofDim[Binary](maxNumRows)
      case _ => sys.error(s"${fieldType} is not supported as a primitive data type")
    }
  private[parquet] def numRows = _numRows
  private[parquet] def numRows_=(size: Int) = _numRows = size
  private[this] def checkMaxSize(size: Int) = if (size > maxNumRows) {
    sys.error(s"Can't support batch size $size, only upto $maxNumRows supported")
  }
  override def getNullIndicatorBatchStore(maxSize: Int) = { checkMaxSize(maxSize); nullFlags }
  override def getBooleanBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize);
    colVals.asInstanceOf[Array[Boolean]]
  }
  override def getIntBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize);
    colVals.asInstanceOf[Array[Int]]
  }
  override def getLongBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize);
    colVals.asInstanceOf[Array[Long]]
  }
  override def getFloatBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize);
    colVals.asInstanceOf[Array[Float]]
  }
  override def getDoubleBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize);
    colVals.asInstanceOf[Array[Double]]
  }
  override def getBinaryBatchStore(maxSize: Int) = {
    checkMaxSize(maxSize);
    colVals.asInstanceOf[Array[Binary]]
  }
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
  private[parquet] def apply(i: Int): Any = {
    if (isNullValue(i)) null else colVals(i)
  }
  private[parquet] def update(i: Int, v: Any): Any = {
    if (v == null) nullFlags(i) = true else {
      nullFlags(i) = false
      colVals.asInstanceOf[Array[Any]](i) = v
    }
  }
  private[parquet] def isNullValue(i: Int) = nullFlags(i)
  private[parquet] def setNullValue(i: Int) = nullFlags(i) = true
  private[parquet] def getBoolean(i: Int) = colVals.asInstanceOf[Array[Boolean]](i)
  private[parquet] def setBoolean(i: Int, v: Boolean) = {
    nullFlags(i) = false
    colVals.asInstanceOf[Array[Boolean]](i) = v
  }
  private[parquet] def getInt(i: Int) = colVals.asInstanceOf[Array[Int]](i)
  private[parquet] def setInt(i: Int, v: Int) = {
    nullFlags(i) = false
    colVals.asInstanceOf[Array[Int]](i) = v
  }
  private[parquet] def getLong(i: Int) = colVals.asInstanceOf[Array[Long]](i)
  private[parquet] def setLong(i: Int, v: Long) = {
    nullFlags(i) = false
    colVals.asInstanceOf[Array[Long]](i) = v
  }
  private[parquet] def getFloat(i: Int) = colVals.asInstanceOf[Array[Float]](i)
  private[parquet] def setFloat(i: Int, v: Float) = {
    nullFlags(i) = false
    colVals.asInstanceOf[Array[Float]](i) = v
  }
  private[parquet] def getDouble(i: Int) = colVals.asInstanceOf[Array[Double]](i)
  private[parquet] def setDouble(i: Int, v: Double) = {
    nullFlags(i) = false
    colVals.asInstanceOf[Array[Double]](i) = v
  }
  private[parquet] def getBinary(i: Int) = colVals.asInstanceOf[Array[Binary]](i)
  private[parquet] def setBinary(i: Int, v: Binary) = {
    if (v == null) nullFlags(i) = true else nullFlags(i) = false
    colVals.asInstanceOf[Array[Binary]](i) = v
  }
  private[parquet] def getString(i: Int): String = throw new UnsupportedOperationException   
  private[parquet] def setString(i: Int, v: String): Unit = throw new UnsupportedOperationException
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
private[parquet] class CatalystBatchBinaryConverter(fieldType: DataType, fieldIndex: Int)
  extends CatalystBatchPrimitiveConverter(fieldType, fieldIndex) {

  private[this] var dict: Array[Array[Byte]] = null
  private[this] var dictLookupBatchStore: Array[Int] = null
  private[this] val convertedVals = new Array[Array[Byte]](colVals.length)
  private[this] val colValsBinary = colVals.asInstanceOf[Array[Binary]]
  private[this] def initConvertedVals() {
    var i = 0
    while (i < colVals.length) {
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
      colValsBinary(i) = Binary.fromString(v)
      nullFlags(i) = false
      convertedVals(i) = null
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
private[parquet] class CatalystBatchStringConverter(fieldType: DataType, fieldIndex: Int)
  extends CatalystBatchPrimitiveConverter(fieldType, fieldIndex) {

  private[this] var dict: Array[String] = null
  private[this] var dictLookupBatchStore: Array[Int] = null
  private[this] val convertedVals = new Array[String](colVals.length)
  private[this] val colValsBinary = colVals.asInstanceOf[Array[Binary]]
  private[this] def initConvertedVals() {
    var i = 0
    while (i < colVals.length) {
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
      val s = v.toStringUsingUTF8
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

