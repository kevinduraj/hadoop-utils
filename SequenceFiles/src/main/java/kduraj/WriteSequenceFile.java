package kduraj;

import java.io.IOException;
import static kduraj.App.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class WriteSequenceFile {
    
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
}
