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
    private final static Charset CP1252 = Charset.forName( "CP1252");

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
        final String stringToDecode = getStringValue(arguments, 0, converters);
        if (stringToDecode == null) {
            return null;
        }

        final char[] charactersToDecode = stringToDecode.toCharArray();
        final byte[] bytesArray = new byte[1];
        StringBuilder utf8_text = new StringBuilder(charactersToDecode.length);
        for (int i = 0; i < charactersToDecode.length; i++){
            char character = charactersToDecode[i];

            if( Character.isISOControl( character)){  // encoding problems happened only with control characters
                int asciiCode = (int)character;
                if( asciiCode >= 1 && asciiCode <= 9 ){
                    utf8_text.append(" ");    // those control characters are transformed to spaces by BigQuery
                }
                else {
                    int codePoint = Character.codePointAt( charactersToDecode,i);
                    // First of all, let's handle the control characters that are transformed to space by BigQuery
                    // (about those characters see: http://www.fileformat.info/info/unicode/char/0081/index.htm,
                    // and http://www.ltg.ed.ac.uk/~richard/utf-8.cgi?input=129&mode=decimal
                    // TODO add missing characters (so far I have just seen those 3)
                    if( codePoint == 129 || codePoint == 144 || codePoint == 157 ){
                        utf8_text.append(" ");
                    }
                    else {
                        // this part tries to solve the cases like:
                        // C299 (hex) CP1252 character being transformed by BigQuery into E284A2 (hex) in UTF8

                        // first we need to get rid of the control character (in our example, we get rid of 'C2' and
                        // we just keep '99')
                        String last_2_hexa_bytes = String.format("%02x", (int) character);
                        // gets the byte array from above "hexa string representation"
                        // (solution extracted from https://stackoverflow.com/a/140861/4064443)
                        bytesArray[0] = (byte) ((Character.digit(last_2_hexa_bytes.charAt(0), 16) << 4)
                                | Character.digit(last_2_hexa_bytes.charAt(1), 16));
                        ByteBuffer wrappedBytes = ByteBuffer.wrap(bytesArray);

                        // now we can properly decode from CP1252 charset
                        String decodedString = CP1252.decode(wrappedBytes).toString();
                        utf8_text.append(decodedString);
                    }
                }
            }
            else{   // "standard" characters are OK. We just copied them "as is"
                utf8_text.append(character);
            }
        }
        output.set(utf8_text.toString());
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
