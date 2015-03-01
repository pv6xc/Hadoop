package com.development.mapreduce.customewritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by achilles on 01/03/15.
 */
public class CustomWritable implements WritableComparable<CustomWritable>{

    String left;
    String right;

    /**
     * Empty constructor for serialization
     */
    public CustomWritable() {

    }

    public CustomWritable(String left, String right) {
        this.left=left;
        this.right=right;
    }

    /**
     * Compare the right word only if left word is same.
     * return compare right
     * @param other
     * @return
     */
    @Override
    public int compareTo(CustomWritable other) {

        int ret = this.left.compareTo(other.left);
        if(ret==0)
            return this.right.compareTo(other.right);
        return ret;
    }

    /**
     * Serialize the fields of this object out
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(left);
        dataOutput.writeUTF(right);

    }


    /**
     * Deseraialize the fields
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {

        left = dataInput.readUTF();
        right = dataInput.readUTF();

    }
    /**
     * A custom method that returns the two strings in the
     * CustomWritable object inside parentheses and separated by
     * a comma. For example: "(left,right)".
     */
    public String toString() {
        return "(" + left + "," + right + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CustomWritable that = (CustomWritable) o;

        if (left != null ? !left.equals(that.left) : that.left != null) return false;
        if (right != null ? !right.equals(that.right) : that.right != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = left != null ? left.hashCode() : 0;
        result = 31 * result + (right != null ? right.hashCode() : 0);
        return result;
    }
}
