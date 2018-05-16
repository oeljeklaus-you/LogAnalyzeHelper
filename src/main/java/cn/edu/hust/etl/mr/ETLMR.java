package cn.edu.hust.etl.mr;

import cn.edu.hust.dataClean.bean.Message;
import cn.edu.hust.dataClean.mr.DataClean;
import cn.edu.hust.etl.bean.IP;
import cn.edu.hust.etl.bean.MessageDetail;
import cn.edu.hust.etl.bean.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;

public class ETLMR {
    static class ETLMapper extends Mapper<LongWritable,Text,MessageDetail,NullWritable>
    {
        static ArrayList<HashMap<Pair<Long,Long>,String>> ips=new ArrayList<HashMap<Pair<Long, Long>, String>>();

        //获取省份和城市
        private static String getProvinceAndCity(String ip)
        {
            String[] splits=ip.split("\\.");
            double value=0;
            for(int i=0;i<splits.length;i++)
            {
                value+=Long.parseLong(splits[i])*Math.pow(2,8*(3-i));
            }
            int high=ips.size()-1;
            int low=0;
            while(low<=high)
            {
                int mid=(low+high)/2;
                Pair<Long,Long> pair=(Pair<Long,Long>)ips.get(mid).keySet().toArray()[0];
                if(value>=pair.getFirst()&&value<=pair.getSecond())
                {
                    return (String)ips.get(mid).values().toArray()[0];
                }
                else if(value>pair.getSecond())
                {
                    low=mid+1;
                }
                else if(value<pair.getFirst()) {
                    high = mid - 1;
                }
            }
            return "未知|未知";
        }

        //获取平台和操作系统
        private static String getPlatformAndOS(String s)
        {
            String[] splits=s.split("\\/");
            if(splits.length<=1) return "others|others";
            StringBuilder sb=new StringBuilder();
            sb.append(splits[0]);
            if(s.toLowerCase().contains("windows"))
            {
                sb.append("|").append("Windows");
            }
            else if(s.toLowerCase().contains("macos"))
            {
                sb.append("|").append("macos");
            }
            else if(s.toLowerCase().contains("linux"))
            {
                sb.append("|").append("linux");
            }
            else
            {
                sb.append("|").append("others");
            }
            return sb.toString();

        }

        // 获取请求的页面
        private static String getRequest_Page(String page)
        {
            String[] splits=page.split("\\?");
            return splits[0];
        }

        //获取请求的主机
        private static String getRefer(String page)
        {
            if("-".equals(page)) return "未知";
            try {
                URI uri=new URI(page);
                String path=uri.getPath();
                return path;
            } catch (URISyntaxException e) {
                e.printStackTrace();
                return "未知";
            }
        }
        /*
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{

            BufferedReader reader=new BufferedReader(new InputStreamReader(new FileInputStream("/home/hadoop/ip.txt")));
            String line;
            try
            {
                while((line=reader.readLine())!=null){
                    String[] splits=line.split("\\|");
                    Long up=Long.parseLong(splits[2]);
                    Long down=Long.parseLong(splits[3]);
                    Pair<Long,Long> pair=new Pair<Long, Long>();
                    pair.setFirst(up);
                    pair.setSecond(down);
                    StringBuilder sb=new StringBuilder();
                    sb.append(splits[6]).append("|"+splits[7]);
                    HashMap<Pair<Long,Long>,String> ip=new HashMap<Pair<Long, Long>, String>();
                    ip.put(pair,sb.toString());
                    ips.add(ip);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


                String[] splits=value.toString().split(",");
                if(splits.length>=9)
                {
                    String year=splits[0].substring(0,4);
                    String month=splits[0].substring(5,7);
                    String day=splits[0].substring(8,10);
                    String hour=splits[0].substring(11,13);
                    String min=splits[0].substring(14,16);
                    String ps=getPlatformAndOS(splits[6]);
                    String platform=ps.split("\\|")[0];
                    String os=ps.split("\\|")[1];
                    String request_url=getRequest_Page(splits[7]);
                    String refer=getRefer(splits[8]);
                    MessageDetail md=new MessageDetail(splits[1],request_url,refer,year,month,day,hour,min,platform,os);
                    context.write(md,NullWritable.get());
                }
                else
                    return;
                /*
                *
                * this.IP = IP;
                    this.request_url = request_url;
                    this.refer = refer;
                    this.year = year;
                    this.month = month;
                    this.day = day;
                    this.hour = hour;
                    this.min = min;
                 this.platform = platform;
                    this.os=os;
                * */



        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);

        job.setJarByClass(ETLMR.class);

        //设置job的mapper和reducer
        job.setMapperClass(ETLMapper.class);
        //job.setReducerClass(WordCountReducer.class);

        //设置mapper过后的细节
        job.setMapOutputKeyClass(IP.class);
        job.setMapOutputValueClass(NullWritable.class);


        //job.addCacheFile(new URI("file:///home/hadoop/ip.txt"));
        job.setNumReduceTasks(0);

        //设置文件输出路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean flag=job.waitForCompletion(true);

        System.exit(flag?0:1);
    }
}
