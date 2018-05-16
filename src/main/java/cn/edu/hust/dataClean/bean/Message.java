package cn.edu.hust.dataClean.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Message implements Writable {
    /**
     * 来源IP
     */
    private String ip;
    /**
     * 访问时间
     */
    private String date;
    /**
     * 请求方式
     */
    private String method;
    /**
     * 请求的url
     */
    private String request_url;
    /**
     * 使用的协议
     */
    private String protocol;
    /**
     * 状态码
     */
    private int status;
    /**
     * 字节数
     */
    private int bytes;
    /**
     * 来源url
     */
    private String from_url;
    /**
     * 使用的平台
     */
    private String platform;


    public Message() {
    }

    public Message(String ip, String date, String method, String request_url, String protocol, int status, int bytes, String from_url, String platform) {
        this.ip = ip;
        this.date = date;
        this.method = method;
        this.request_url = request_url;
        this.protocol = protocol;
        this.status = status;
        this.bytes = bytes;
        this.from_url = from_url;
        this.platform = platform;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getRequest_url() {
        return request_url;
    }

    public void setRequest_url(String request_url) {
        this.request_url = request_url;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getBytes() {
        return bytes;
    }

    public void setBytes(int bytes) {
        this.bytes = bytes;
    }

    public String getFrom_url() {
        return from_url;
    }

    public void setFrom_url(String from_url) {
        this.from_url = from_url;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    @Override
    public String toString() {
        return date +
                "," + ip +
                "," + method +
                "," + protocol+
                "," + status +
                "," + bytes +","+platform+
                "," +request_url +
                "," +from_url;
    }
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ip);
        dataOutput.writeUTF(this.date);
        dataOutput.writeUTF(this.method);
        dataOutput.writeUTF(this.request_url);
        dataOutput.writeUTF(this.protocol);
        dataOutput.writeInt(this.status);
        dataOutput.writeInt(this.bytes);
        dataOutput.writeUTF(this.from_url);
        dataOutput.writeUTF(this.platform);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.ip=dataInput.readUTF();
        this.date=dataInput.readUTF();
        this.method=dataInput.readUTF();
        this.request_url=dataInput.readUTF();
        this.protocol=dataInput.readUTF();
        this.status=dataInput.readInt();
        this.bytes=dataInput.readInt();
        this.from_url=dataInput.readUTF();
        this.platform=dataInput.readUTF();
    }
}
