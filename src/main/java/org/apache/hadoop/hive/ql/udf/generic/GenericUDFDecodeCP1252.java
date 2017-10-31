/**
 *
 * Copyright 2017 bol.com. All Rights Reserved
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

/**
 * UDFDecodeCP1252.
 */
@Description(
        name = "UDFDecodeCP1252",
        value = "_FUNC_(str) - Returns str, which is the same string but in a Google's UTF8 encoding instead of the "
                + "initial CP1252 encoding")
public class GenericUDFDecodeCP1252 extends GenericUDF {
    private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];
    private transient Converter[] converters = new Converter[1];
    private final Text output = new Text();
    private final Charset cp1252 = Charset.forName( "CP1252");

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 1, 1);

        checkArgPrimitive(arguments, 0);

        checkArgGroups(arguments, 0, inputTypes, STRING_GROUP);

        obtainStringConverter(arguments, 0, inputTypes, converters);

        ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        return outputOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String val = getStringValue(arguments, 0, converters);
        if (val == null) {
            return null;
        }

        char[] myCharArray = val.toCharArray();
        byte[] bytesArray = new byte[1];
        StringBuilder buf = new StringBuilder(myCharArray.length);
        for (int i = 0; i < myCharArray.length; i++){
            char ch = myCharArray[i];
            if( Character.isISOControl( ch)){
                int chint = (int)ch;
                if( chint >= 1 && chint <= 9 ){
                    buf.append(" ");    // those control characters are transformed to spaces by BigQuery
                }
                else {
                    int codePoint = Character.codePointAt( myCharArray,i);
                    if( codePoint == 129 || codePoint == 144 || codePoint == 157 ){
                        buf.append(" ");    // those control character transformed to space by BigQuery
                    }
                    else {
                        String hexa = String.format("%02x", (int) ch); // this is where the control characters
                        // (like \xc2 in \xc2\x99) are removed
                        bytesArray[0] = (byte) ((Character.digit(hexa.charAt(0), 16) << 4)
                                | Character.digit(hexa.charAt(1), 16));
                        ByteBuffer wrappedBytes = ByteBuffer.wrap(bytesArray);
                        String decodedString = cp1252.decode(wrappedBytes).toString();
                        buf.append(decodedString);
                    }
                }
            }
            else{
                buf.append(ch);
            }
        }
        output.set(buf.toString());
        return output;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString(getFuncName(), children);
    }

    @Override
    protected String getFuncName() {
        return "decodeCP1252";
    }
}
