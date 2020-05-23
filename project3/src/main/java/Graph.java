import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
/*
rm -rf inter
~/hadoop-2.6.5/bin/hadoop jar target/cse6331-P3-0.1.jar Graph small-graph.txt inter output
*/

class Vertex implements Writable {
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    Vertex(short tag,long group,long VID,Vector adjacent)
    {
        this.tag=tag;
        this.group=group;
        this.VID=VID;
        this.adjacent=adjacent;
    }
    Vertex()
    { 
    }
    Vertex(short tag,long group)
    {
        this.tag=tag;
        this.group=group;
        this.VID=0;
        this.adjacent=new Vector();
    } 
    public void print()
    {
        System.out.println(tag+","+group+","+VID+","+adjacent);
    }
    public void write ( DataOutput out ) throws IOException {
        out.writeShort(this.tag);
        out.writeLong(this.group);
        out.writeLong(this.VID);
        out.writeInt(adjacent.size());
        for(int i=0;i<adjacent.size();i++)
        { 
            out.writeLong(adjacent.get(i));
        }
    }
    public void readFields ( DataInput in ) throws IOException {
        tag=in.readShort();
        group=in.readLong();
        VID=in.readLong();
        int x=in.readInt(); 
        Vector<Long> a=new Vector();
        for(int i=0;i<x;i++)
        {
            long dummy=in.readLong(); 
            a.addElement(dummy);
        }
        adjacent=a;
    } 
}

public class Graph {

    public static class Mapper1 extends Mapper<Object,Text,LongWritable,Vertex> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Vector<Long> v =new Vector();
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int x=0;
            Integer zero=0;
            long vid=0;
            Integer ivid=0;
            while(s.hasNext())
            {

                ivid=s.nextInt();
                if(x==0)
                {
                    vid=ivid.longValue();
                    x=1;
                }
                else
                {
                    long avid=ivid.longValue();
                    v.add(avid);
                } 
            }
            // System.out.println();
            Vertex v1=new Vertex(zero.shortValue(),vid,vid,v); 
            context.write(new LongWritable(vid),v1);
            s.close();
        }
    }
    public static class Mapper2 extends Mapper<LongWritable,Vertex,LongWritable,Vertex> { 
        public void map ( LongWritable key, Vertex v, Context context )
                        throws IOException, InterruptedException { 
            // System.out.println("debug-size"+v.adjacent.size());
            context.write(new LongWritable(v.VID),v);
            int size=v.adjacent.size();
            for(int i=0;i<size;i++)
            {
                // System.out.println("debug adj:"+v.adjacent.get(i));
                // System.out.println("debug gourp:"+v.group);
                short s=1;
                context.write(new LongWritable(v.adjacent.get(i)),new Vertex(s,v.group));
            }
        }
    }
    public static long min(long a,long b)
    {
        if(a<b)
        {
            return a;
        }
        else
        {
            return b;
        }
    }
    public static class Reducer2 extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
 
        public void reduce ( LongWritable vid, Iterable<Vertex> values, Context context)
                           throws IOException, InterruptedException {
            long m=Long.MAX_VALUE;
            Vector<Long> adj =new Vector();
            for(Vertex v:values) 
            {
                if(v.tag==0)
                {
                    adj=v.adjacent;
                }
                m=min(m,v.group);
            }
            short s=0;
            context.write(new LongWritable(m),new Vertex(s,m,vid.get(),adj));
        }
    }
    public static class Mapper3 extends Mapper<LongWritable,Vertex,LongWritable,LongWritable> { 
        public void map ( LongWritable key, Vertex v, Context context )
                        throws IOException, InterruptedException {
            context.write(key,new LongWritable(1));
        }
    }
    public static class Reducer3 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
 
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context)
                           throws IOException, InterruptedException {
            long m=0;
            for(LongWritable v:values) 
            {
                 m=m+v.get();
            } 
            context.write(key,new LongWritable(m));
        }
    }
    public static void main ( String[] args ) throws Exception {
        Job job1 = Job.getInstance();
        job1.setJobName("MyJob1");
        job1.setJarByClass(Graph.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        job1.setMapperClass(Mapper1.class); 
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[0])); 
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/f0"));
        job1.waitForCompletion(true);
        for ( short i = 0; i < 5; i++ ) {
            Job job2 = Job.getInstance();
            job2.setJobName("MyJob2");
            job2.setJarByClass(Graph.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
            job2.setMapperClass(Mapper2.class); 
            job2.setReducerClass(Reducer2.class);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job2,new Path(args[1]+"/f"+i)); 
            FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/f"+(i+1)));
            job2.waitForCompletion(true);
        }
        Job job3 = Job.getInstance();
        job3.setJobName("MyJob3");
        job3.setJarByClass(Graph.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(Vertex.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setMapperClass(Mapper3.class); 
        job3.setReducerClass(Reducer3.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3,new Path(args[1]+"/f5")); 
        FileOutputFormat.setOutputPath(job3,new Path(args[2]));
        job3.waitForCompletion(true);
    }
}

 