package kduraj;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class App {

    /*--------------------------------------------------------------------------------------------*/
    String[] DATA = {
        "one, first line", "two, second line", "tree, third line",
        "four, forth line", "five, fifth line", "six, sixth line",
        "four, forth line", "five, fifth line", "six, sixth line",};

    static String uri;
    static Configuration conf;
    static FileSystem fs;
    static Path path;

    /*--------------------------------------------------------------------------------------------    
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
     --------------------------------------------------------------------------------------------*/
    public static void main(String[] args) throws IOException {
        
        uri = args[0];
        conf = new Configuration();
        fs = FileSystem.get(URI.create(uri), conf);
        path = new Path(uri);
        
        App app = new App();        
        app.key_val_bytes();
    }

    /*--------------------------------------------------------------------------------------------*/
    public void key_val_bytes() throws IOException {

        BytesWritable key = new BytesWritable();
        Text value = new Text();

        SequenceFile.Writer writer = null;

        try {
            writer = new SequenceFile.Writer(fs, conf, path, key.getClass(), value.getClass());

            for (int i = 0; i < DATA.length; i++) {

                String[] parts = DATA[i].split(",");
                writer.append(new BytesWritable(parts[0].getBytes()), value);

            }

        } catch (Exception ex) {
            System.out.println("Error writing: " + ex.getMessage());
        } finally {
            IOUtils.closeStream(writer);
        }
    }
    /*--------------------------------------------------------------------------------------------*/

    public void key_val_text(String[] args) throws IOException {

        Text key = new Text();
        Text value = new Text();

        SequenceFile.Writer writer = null;

        try {
            writer = new SequenceFile.Writer(fs, conf, path, key.getClass(), value.getClass());

            for (int i = 0; i < DATA.length; i++) {

                String[] parts = DATA[i].split(",");
                key.set(parts[0]);
                value.set(DATA[i % DATA.length]);

                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);

            }

        } catch (Exception ex) {
            System.out.println("Error writing: " + ex.getMessage());
        } finally {
            IOUtils.closeStream(writer);
        }
    }
    /*--------------------------------------------------------------------------------------------*/

    public void key_val_integer(String[] args) throws IOException {

        IntWritable key = new IntWritable();
        Text value = new Text();

        SequenceFile.Writer writer = null;

        try {
            writer = new SequenceFile.Writer(fs, conf, path, key.getClass(), value.getClass());

            for (int i = 0; i < DATA.length; i++) {

                String[] parts = DATA[i].split(",");
                key.set(i);
                value.set(DATA[i % DATA.length]);

                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);

            }

        } catch (Exception ex) {
            System.out.println("Error writing: " + ex.getMessage());
        } finally {
            IOUtils.closeStream(writer);
        }
    }
    /*--------------------------------------------------------------------------------------------*/
}
