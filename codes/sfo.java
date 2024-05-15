/*Import all Java Libraries
**	Libraries imported from:
**	Hadoop, Cloudera, Java
*/
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

//Sample Sqoop Java Code

public class sfo extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  //Set Mapper to ensure columns and their corresponding datatypes
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  
  //Initialize data fields
  //Similar to create table command in MySQL
  private void init0() {
    setters.put("Date", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        sfo.this.Date = (java.sql.Date)value;
      }
    });
    setters.put("High_Temp", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        sfo.this.High_Temp = (Integer)value;
      }
    });
    setters.put("Low_Temp", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        sfo.this.Low_Temp = (Integer)value;
      }
    });
    setters.put("Precipitation", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        sfo.this.Precipitation = (Float)value;
      }
    });
    setters.put("Dew_Point", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        sfo.this.Dew_Point = (Float)value;
      }
    });
    setters.put("Wind", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        sfo.this.Wind = (Integer)value;
      }
    });
    setters.put("Sea_Level", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        sfo.this.Sea_Level = (Float)value;
      }
    });
    setters.put("Day_Length", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        sfo.this.Day_Length = (Integer)value;
      }
    });
  }
  public sfo() {
    init0();
  }
  
  /*
  **	Getter & Setter method for getting date values
  **	Similar to methods in Data Abstration techniques in OOPs
  */
  
  private java.sql.Date Date;
  public java.sql.Date get_Date() {
    return Date;
  }
  public void set_Date(java.sql.Date Date) {
    this.Date = Date;
  }
  public sfo with_Date(java.sql.Date Date) {
    this.Date = Date;
    return this;
  }
  private Integer High_Temp;
  public Integer get_High_Temp() {
    return High_Temp;
  }
  public void set_High_Temp(Integer High_Temp) {
    this.High_Temp = High_Temp;
  }
  public sfo with_High_Temp(Integer High_Temp) {
    this.High_Temp = High_Temp;
    return this;
  }
  private Integer Low_Temp;
  public Integer get_Low_Temp() {
    return Low_Temp;
  }
  public void set_Low_Temp(Integer Low_Temp) {
    this.Low_Temp = Low_Temp;
  }
  public sfo with_Low_Temp(Integer Low_Temp) {
    this.Low_Temp = Low_Temp;
    return this;
  }
  private Float Precipitation;
  public Float get_Precipitation() {
    return Precipitation;
  }
  public void set_Precipitation(Float Precipitation) {
    this.Precipitation = Precipitation;
  }
  public sfo with_Precipitation(Float Precipitation) {
    this.Precipitation = Precipitation;
    return this;
  }
  private Float Dew_Point;
  public Float get_Dew_Point() {
    return Dew_Point;
  }
  public void set_Dew_Point(Float Dew_Point) {
    this.Dew_Point = Dew_Point;
  }
  public sfo with_Dew_Point(Float Dew_Point) {
    this.Dew_Point = Dew_Point;
    return this;
  }
  private Integer Wind;
  public Integer get_Wind() {
    return Wind;
  }
  public void set_Wind(Integer Wind) {
    this.Wind = Wind;
  }
  public sfo with_Wind(Integer Wind) {
    this.Wind = Wind;
    return this;
  }
  private Float Sea_Level;
  public Float get_Sea_Level() {
    return Sea_Level;
  }
  public void set_Sea_Level(Float Sea_Level) {
    this.Sea_Level = Sea_Level;
  }
  public sfo with_Sea_Level(Float Sea_Level) {
    this.Sea_Level = Sea_Level;
    return this;
  }
  private Integer Day_Length;
  public Integer get_Day_Length() {
    return Day_Length;
  }
  public void set_Day_Length(Integer Day_Length) {
    this.Day_Length = Day_Length;
  }
  public sfo with_Day_Length(Integer Day_Length) {
    this.Day_Length = Day_Length;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof sfo)) {
      return false;
    }
    sfo that = (sfo) o;
    boolean equal = true;
    equal = equal && (this.Date == null ? that.Date == null : this.Date.equals(that.Date));
    equal = equal && (this.High_Temp == null ? that.High_Temp == null : this.High_Temp.equals(that.High_Temp));
    equal = equal && (this.Low_Temp == null ? that.Low_Temp == null : this.Low_Temp.equals(that.Low_Temp));
    equal = equal && (this.Precipitation == null ? that.Precipitation == null : this.Precipitation.equals(that.Precipitation));
    equal = equal && (this.Dew_Point == null ? that.Dew_Point == null : this.Dew_Point.equals(that.Dew_Point));
    equal = equal && (this.Wind == null ? that.Wind == null : this.Wind.equals(that.Wind));
    equal = equal && (this.Sea_Level == null ? that.Sea_Level == null : this.Sea_Level.equals(that.Sea_Level));
    equal = equal && (this.Day_Length == null ? that.Day_Length == null : this.Day_Length.equals(that.Day_Length));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof sfo)) {
      return false;
    }
    sfo that = (sfo) o;
    boolean equal = true;
    equal = equal && (this.Date == null ? that.Date == null : this.Date.equals(that.Date));
    equal = equal && (this.High_Temp == null ? that.High_Temp == null : this.High_Temp.equals(that.High_Temp));
    equal = equal && (this.Low_Temp == null ? that.Low_Temp == null : this.Low_Temp.equals(that.Low_Temp));
    equal = equal && (this.Precipitation == null ? that.Precipitation == null : this.Precipitation.equals(that.Precipitation));
    equal = equal && (this.Dew_Point == null ? that.Dew_Point == null : this.Dew_Point.equals(that.Dew_Point));
    equal = equal && (this.Wind == null ? that.Wind == null : this.Wind.equals(that.Wind));
    equal = equal && (this.Sea_Level == null ? that.Sea_Level == null : this.Sea_Level.equals(that.Sea_Level));
    equal = equal && (this.Day_Length == null ? that.Day_Length == null : this.Day_Length.equals(that.Day_Length));
    return equal;
  }
  
  /*
  **	Create JDBC Readble methods for different Columns present in the table
  */
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.Date = JdbcWritableBridge.readDate(1, __dbResults);
    this.High_Temp = JdbcWritableBridge.readInteger(2, __dbResults);
    this.Low_Temp = JdbcWritableBridge.readInteger(3, __dbResults);
    this.Precipitation = JdbcWritableBridge.readFloat(4, __dbResults);
    this.Dew_Point = JdbcWritableBridge.readFloat(5, __dbResults);
    this.Wind = JdbcWritableBridge.readInteger(6, __dbResults);
    this.Sea_Level = JdbcWritableBridge.readFloat(7, __dbResults);
    this.Day_Length = JdbcWritableBridge.readInteger(8, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.Date = JdbcWritableBridge.readDate(1, __dbResults);
    this.High_Temp = JdbcWritableBridge.readInteger(2, __dbResults);
    this.Low_Temp = JdbcWritableBridge.readInteger(3, __dbResults);
    this.Precipitation = JdbcWritableBridge.readFloat(4, __dbResults);
    this.Dew_Point = JdbcWritableBridge.readFloat(5, __dbResults);
    this.Wind = JdbcWritableBridge.readInteger(6, __dbResults);
    this.Sea_Level = JdbcWritableBridge.readFloat(7, __dbResults);
    this.Day_Length = JdbcWritableBridge.readInteger(8, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  /*
  ** Create JDBC Writeable methods for different Columns present in the table
  */
  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeDate(Date, 1 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeInteger(High_Temp, 2 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(Low_Temp, 3 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeFloat(Precipitation, 4 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeFloat(Dew_Point, 5 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeInteger(Wind, 6 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeFloat(Sea_Level, 7 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeInteger(Day_Length, 8 + __off, 4, __dbStmt);
    return 8;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeDate(Date, 1 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeInteger(High_Temp, 2 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(Low_Temp, 3 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeFloat(Precipitation, 4 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeFloat(Dew_Point, 5 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeInteger(Wind, 6 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeFloat(Sea_Level, 7 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeInteger(Day_Length, 8 + __off, 4, __dbStmt);
  }
  
  /*
  ** Check for NULL values in data and program it to handle them carefully.
  */
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.Date = null;
    } else {
    this.Date = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.High_Temp = null;
    } else {
    this.High_Temp = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.Low_Temp = null;
    } else {
    this.Low_Temp = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.Precipitation = null;
    } else {
    this.Precipitation = Float.valueOf(__dataIn.readFloat());
    }
    if (__dataIn.readBoolean()) { 
        this.Dew_Point = null;
    } else {
    this.Dew_Point = Float.valueOf(__dataIn.readFloat());
    }
    if (__dataIn.readBoolean()) { 
        this.Wind = null;
    } else {
    this.Wind = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.Sea_Level = null;
    } else {
    this.Sea_Level = Float.valueOf(__dataIn.readFloat());
    }
    if (__dataIn.readBoolean()) { 
        this.Day_Length = null;
    } else {
    this.Day_Length = Integer.valueOf(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.Date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.Date.getTime());
    }
    if (null == this.High_Temp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.High_Temp);
    }
    if (null == this.Low_Temp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.Low_Temp);
    }
    if (null == this.Precipitation) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.Precipitation);
    }
    if (null == this.Dew_Point) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.Dew_Point);
    }
    if (null == this.Wind) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.Wind);
    }
    if (null == this.Sea_Level) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.Sea_Level);
    }
    if (null == this.Day_Length) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.Day_Length);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.Date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.Date.getTime());
    }
    if (null == this.High_Temp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.High_Temp);
    }
    if (null == this.Low_Temp) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.Low_Temp);
    }
    if (null == this.Precipitation) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.Precipitation);
    }
    if (null == this.Dew_Point) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.Dew_Point);
    }
    if (null == this.Wind) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.Wind);
    }
    if (null == this.Sea_Level) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.Sea_Level);
    }
    if (null == this.Day_Length) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.Day_Length);
    }
  }
  
  /*
  ** Create a builder for injecting data into HDFS.
  ** Similar to String Concatination done in C++, Python
  ** Performed in Mapper stage to create key value pairs for each entry in the database
  */
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 1, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(Date==null?"null":"" + Date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(High_Temp==null?"null":"" + High_Temp, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Low_Temp==null?"null":"" + Low_Temp, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Precipitation==null?"null":"" + Precipitation, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Dew_Point==null?"null":"" + Dew_Point, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Wind==null?"null":"" + Wind, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Sea_Level==null?"null":"" + Sea_Level, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Day_Length==null?"null":"" + Day_Length, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(Date==null?"null":"" + Date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(High_Temp==null?"null":"" + High_Temp, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Low_Temp==null?"null":"" + Low_Temp, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Precipitation==null?"null":"" + Precipitation, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Dew_Point==null?"null":"" + Dew_Point, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Wind==null?"null":"" + Wind, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Sea_Level==null?"null":"" + Sea_Level, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Day_Length==null?"null":"" + Day_Length, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 1, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Date = null; } else {
      this.Date = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.High_Temp = null; } else {
      this.High_Temp = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Low_Temp = null; } else {
      this.Low_Temp = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Precipitation = null; } else {
      this.Precipitation = Float.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Dew_Point = null; } else {
      this.Dew_Point = Float.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Wind = null; } else {
      this.Wind = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Sea_Level = null; } else {
      this.Sea_Level = Float.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Day_Length = null; } else {
      this.Day_Length = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Date = null; } else {
      this.Date = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.High_Temp = null; } else {
      this.High_Temp = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Low_Temp = null; } else {
      this.Low_Temp = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Precipitation = null; } else {
      this.Precipitation = Float.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Dew_Point = null; } else {
      this.Dew_Point = Float.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Wind = null; } else {
      this.Wind = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Sea_Level = null; } else {
      this.Sea_Level = Float.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.Day_Length = null; } else {
      this.Day_Length = Integer.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    sfo o = (sfo) super.clone();
    o.Date = (o.Date != null) ? (java.sql.Date) o.Date.clone() : null;
    return o;
  }

  public void clone0(sfo o) throws CloneNotSupportedException {
    o.Date = (o.Date != null) ? (java.sql.Date) o.Date.clone() : null;
  }
  /*
  ** Put metadata on hive metastore database
  */
  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("Date", this.Date);
    __sqoop$field_map.put("High_Temp", this.High_Temp);
    __sqoop$field_map.put("Low_Temp", this.Low_Temp);
    __sqoop$field_map.put("Precipitation", this.Precipitation);
    __sqoop$field_map.put("Dew_Point", this.Dew_Point);
    __sqoop$field_map.put("Wind", this.Wind);
    __sqoop$field_map.put("Sea_Level", this.Sea_Level);
    __sqoop$field_map.put("Day_Length", this.Day_Length);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("Date", this.Date);
    __sqoop$field_map.put("High_Temp", this.High_Temp);
    __sqoop$field_map.put("Low_Temp", this.Low_Temp);
    __sqoop$field_map.put("Precipitation", this.Precipitation);
    __sqoop$field_map.put("Dew_Point", this.Dew_Point);
    __sqoop$field_map.put("Wind", this.Wind);
    __sqoop$field_map.put("Sea_Level", this.Sea_Level);
    __sqoop$field_map.put("Day_Length", this.Day_Length);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
