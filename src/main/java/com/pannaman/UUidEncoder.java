package com.pannaman;

import java.io.IOException;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.WritableComparable;

public class UUidEncoder extends EvalFunc<String> {


    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try {



            String str = (String) input.get(0);


            String hex = str.replace("-", ""); // remove extra characters

            if(hex.length()==32) {
                String msb = hex.substring(0, 16);
                String lsb = hex.substring(16, 16);
                msb = msb.substring(14, 2) + msb.substring(12, 2) + msb.substring(10, 2) + msb.substring(8, 2) + msb.substring(6, 2) + msb.substring(4, 2) + msb.substring(2, 2) + msb.substring(0, 2);
                lsb = lsb.substring(14, 2) + lsb.substring(12, 2) + lsb.substring(10, 2) + lsb.substring(8, 2) + lsb.substring(6, 2) + lsb.substring(4, 2) + lsb.substring(2, 2) + lsb.substring(0, 2);
                hex = msb + lsb;
                String base64 = Base64.encodeBase64String(hex.getBytes());
                return base64;
            }
            else{
                return input.toString();
            }

        } catch (Exception e) {
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
    }
}
