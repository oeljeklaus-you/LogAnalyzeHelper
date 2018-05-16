package cn.edu.hust.dataClean.mr;

import cn.edu.hust.dataClean.bean.Message;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DataClean {
    static class DataCleanMapper extends Mapper<LongWritable,Text,Message,NullWritable>
    {
        public static String formatDate(String dateStr) {
            if (dateStr == null || StringUtils.isBlank(dateStr)) return "2012-04-04 12.00.00";
            SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
            SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.CHINA);
            String result = null;
            try {
                Date date = format.parse(dateStr);
                result = format1.format(date);
            } catch (ParseException e) {
                e.printStackTrace();
            } finally {
                return result;
            }

        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try
            {
                String message=value.toString();
                String[] splits=message.split(" ");
                if(splits.length<12) return;
                String time=formatDate(splits[3].substring(1));
                String method=splits[5].substring(1);
                String protocol=StringUtils.isBlank(splits[7])?"HTTP/1.1":splits[7].substring(0,splits[7].length()-1);
                int status= StringUtils.isBlank(splits[8])?0:Integer.parseInt(splits[8]);
                int bytes=StringUtils.isBlank(splits[9])?0:Integer.parseInt(splits[9]);
                String from_url=StringUtils.isBlank(splits[9])?"":splits[10].substring(1,splits[10].length()-1);
                StringBuilder sb=new StringBuilder();
                for (int i=11;i<splits.length;i++)
                {
                    sb.append(splits[i]);
                }
                String s=sb.toString();
                String platform=s.substring(1,s.length()-1);
                Message ms=new Message(splits[0],time,method,splits[6],protocol,status,bytes,from_url,platform);
                context.write(ms,NullWritable.get());
            }catch (Exception e){
                return ;
            }


        }


    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);

        job.setJarByClass(DataClean.class);

        //设置job的mapper和reducer
        job.setMapperClass(DataCleanMapper.class);
        //job.setReducerClass(WordCountReducer.class);

        //设置mapper过后的细节
        job.setMapOutputKeyClass(Message.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(0);

        //设置文件输出路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean flag=job.waitForCompletion(true);

        System.exit(flag?0:1);
    }

}
