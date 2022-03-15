package lesson.bigdata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UsageBean implements WritableComparable<UsageBean> {
    private String phoneNum;
    private long upStream;
    private long downStream;
    private long sumUsage;

    public UsageBean(){}

    public UsageBean(String phoneNum,long upStream,long downStream) {
        super();
        this.phoneNum = phoneNum;
        this.upStream = upStream;
        this.downStream = downStream;
        this.sumUsage = upStream + downStream;
    }

    public void readFields(DataInput input) throws IOException{
        phoneNum = input.readUTF();
        upStream = input.readLong();
        downStream = input.readLong();
        sumUsage = input.readLong();
    }

    public void write(DataOutput output) throws IOException{
        output.writeUTF(phoneNum);
        output.writeLong(upStream);
        output.writeLong(downStream);
        output.writeLong(sumUsage);
    }
    
    @Override
    public String toString() {
        return ""+upStream+"\t"+downStream+"\t"+sumUsage;
    }

   @Override
   public int compareTo(UsageBean o) {
      return sumUsage > o.sumUsage ? -1 : 1;
   }

   public String getPhoneNum() {
    return this.phoneNum;
}

public void setPhoneNum(String phoneNum) {
    this.phoneNum = phoneNum;
}

public long getUpStream() {
    return this.upStream;
}

public void setUpStream(long upStream) {
    this.upStream = upStream;
}

public long getDownStream() {
    return this.downStream;
}

public void setDownStream(long downStream) {
    this.downStream = downStream;
}

public long getSumUsage() {
    return this.sumUsage;
}

public void setSumUsage(long sumUsage) {
    this.sumUsage = sumUsage;
}
    
}