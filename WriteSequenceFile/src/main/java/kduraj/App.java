package kduraj;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

public class App {

    /*--------------------------------------------------------------------------------------------*/
    String[] DATA = {
          "key1|one"
	, "key2|two"
	, "key3|three"
	, "key4|four"
	, "key9|nine"
	, "key5|five"
	, "key6|six"
	, "key7|seven"
	, "key8|eight"
	};

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
	app.key_val_reader();

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
                value.set(DATA[i]);

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
    public void key_val_reader() throws IOException {

        SequenceFile.Reader reader = null;

        try {

            reader = new SequenceFile.Reader(fs, path, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();

            while (reader.next(key, value)) {

                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition();
            }

        } catch (Exception ex) {
            System.out.println("ex = " + ex);
        } finally {
            IOUtils.closeStream(reader);
        }
    }
    /*--------------------------------------------------------------------------------------------*/

}
