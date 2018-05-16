package cn.edu.hust.etl.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MessageDetail implements Writable{
    //请求的IP
    private String IP;
    //请求的url
    private String request_url;
    //来源URL
    private String refer;
    //请求的年
    private String year;
    //月
    private String month;
    //日
    private String day;
    //小时
    private String hour;
    //
    private String min;
    //请求的平台
    private String platform;
    //操作系统
    private String os;

    public MessageDetail() {
    }

    public MessageDetail(String IP, String request_url, String refer, String year, String month, String day, String hour, String min, String platform,String os) {
        this.IP = IP;
        this.request_url = request_url;
        this.refer = refer;
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.min = min;
        this.platform = platform;
        this.os=os;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.IP);
        dataOutput.writeUTF(this.request_url);
        dataOutput.writeUTF(this.refer);
        dataOutput.writeUTF(this.year);
        dataOutput.writeUTF(this.month);
        dataOutput.writeUTF(this.day);
        dataOutput.writeUTF(this.hour);
        dataOutput.writeUTF(this.min);
        dataOutput.writeUTF(this.platform);
        dataOutput.writeUTF(this.os);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.IP=dataInput.readUTF();
        this.request_url=dataInput.readUTF();
        this.refer=dataInput.readUTF();
        this.year=dataInput.readUTF();
        this.month=dataInput.readUTF();
        this.day=dataInput.readUTF();
        this.hour=dataInput.readUTF();
        this.min=dataInput.readUTF();
        this.platform=dataInput.readUTF();
        this.os=dataInput.readUTF();
    }

    @Override
    public String toString() {
        return
                IP +
                "," + request_url +
                "," + refer+
                "," +year +
                "," + month +
                "," + day +
                "," + hour+
                "," + min +
                "," + platform+
                "," + os;
    }
}
