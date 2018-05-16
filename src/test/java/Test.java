import cn.edu.hust.etl.bean.Pair;
import cn.edu.hust.etl.mr.ETLMR;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;

public class Test {
    //获取平台和操作系统
    private static String getPlatformAndOS(String s)
    {
        String[] splits=s.split("\\/");
        if(splits.length<=1) return "others|others";
        StringBuilder sb=new StringBuilder();
        sb.append(splits[0]);
        if(s.toLowerCase().equals("windows"))
        {
            sb.append("|").append("Windows");
        }
        else if(s.toLowerCase().equals("macos"))
        {
            sb.append("|").append("macos");
        }
        else if(s.toLowerCase().equals("linux"))
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

    static ArrayList<HashMap<Pair<Long,Long>,String>> ips=new ArrayList<HashMap<Pair<Long,Long>, String>>();

    static {
        InputStreamReader isReader=new InputStreamReader(Test.class.getClassLoader().getResourceAsStream("ip.txt"));
        BufferedReader reader=new BufferedReader(isReader);
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


    @org.junit.Test
    public void testGetOS() throws URISyntaxException {
        String s=getPlatformAndOS("Mozilla/5.0(compatible;Googlebot/2.1;+http://www.google.com/bot.html),");
        System.out.println(s);

        String[] splits="2012-01-04 00:25:56,113.71.137.55,GET,HTTP/1.1,200,13203,Mozilla/4.0(compatible;MSIE6.0;WindowsNT5.1;SV1),/,-".split(",");
        String page=getRequest_Page("/thread-535915-1-1.html");
        System.out.println(page);

        String t=getProvinceAndCity("203.208.60.173");
        System.out.println(t.split("\\|")[0]);
        System.out.println(t.split("\\|")[1]);

        System.out.println(new URI("file:///home/hadoop/ip.txt").toString());
    }
}
