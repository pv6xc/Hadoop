package com.developer.mapreduce.apachecustomwritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by achilles on 01/03/15.
 */
public class ApacheCustomWritable implements WritableComparable<ApacheCustomWritable>{

    String ip;
    String reqPage ;
    String timeStamp ;

    /**
     * Default for serialization
     */
    public ApacheCustomWritable() {
    }

    public ApacheCustomWritable(String ip, String reqPage, String timeStamp) {
        this.ip = ip;
        this.reqPage = reqPage;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "(" + ip + "\t" + reqPage + "\t" + timeStamp + ")";
    }

    /**
     * Check ip , request page and timestamp
     * @param obj
     * @return
     */
    @Override
    public int compareTo(ApacheCustomWritable obj) {

        int ret1 = this.ip.compareTo(obj.ip);
        if (ret1 == 0) {
            int ret2 = this.reqPage.compareTo(obj.reqPage);
            if (ret2 == 0) {
                return this.timeStamp.compareTo(obj.timeStamp);
            } else {
                return ret2;
            }
        } else {
            return ret1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(ip);
            dataOutput.writeUTF(reqPage);
            dataOutput.writeUTF(timeStamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
            this.ip = dataInput.readUTF();
            this.reqPage = dataInput.readUTF();
            this.timeStamp = dataInput.readUTF();
    }
}
