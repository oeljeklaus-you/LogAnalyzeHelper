package cn.edu.hust.etl.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IP implements Writable{
    private Long first;
    private Long second;
    private String content;

    public IP(Long first, Long second, String content) {
        this.first = first;
        this.second = second;
        this.content = content;
    }

    public Long getFirst() {
        return first;
    }

    public void setFirst(Long first) {
        this.first = first;
    }

    public Long getSecond() {
        return second;
    }

    public void setSecond(Long second) {
        this.second = second;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.first);
        dataOutput.writeLong(this.second);
        dataOutput.writeUTF(this.content);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.first=dataInput.readLong();
        this.second=dataInput.readLong();
        this.content=dataInput.readUTF();
    }
}
