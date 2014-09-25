package kduraj;

import java.io.IOException;
import static kduraj.App.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class ReadSequenceFile {
    
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
