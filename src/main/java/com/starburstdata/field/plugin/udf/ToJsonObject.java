/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starburstdata.field.plugin.udf;

import com.fasterxml.jackson.core.JsonGenerator;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.SingleRowBlock;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.CharType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.trino.operator.scalar.JsonOperators.JSON_FACTORY;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.JsonGeneratorWriter.createJsonGeneratorWriter;
import static io.trino.util.JsonUtil.canCastToJson;
import static io.trino.util.JsonUtil.createJsonGenerator;

public final class ToJsonObject
{
    private ToJsonObject() {}

    @ScalarFunction("to_json_object")
    @TypeParameter("V")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice jsonSerialize(
            //@TypeParameter("array(K)") ArrayType keyType,
            @TypeParameter("V") RowType type,
            @SqlType("V") Block value)
    {
        checkCondition(type.getTypeParameters().size() % 2 == 0, INVALID_FUNCTION_ARGUMENT, "Odd number of arguments found. A value must be supplied for every key.");
        checkCondition(canCastToJson(type), INVALID_FUNCTION_ARGUMENT, "Cannot convert %s to JSON", type);
        SliceOutput output = new DynamicSliceOutput(40);

        try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
            addNestedRows(jsonGenerator, type, value);
            jsonGenerator.close();
            return output.slice();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void addNestedRows(JsonGenerator jsonGenerator, Type type, Block row)
    {
        try {
            jsonGenerator.writeStartObject();
            int index = 0;
            for (Type t : type.getTypeParameters()) {
                if (index % 2 == 0) {
                    checkCondition(t instanceof CharType || t instanceof VarcharType, INVALID_FUNCTION_ARGUMENT, "Key at index %d is not a textual type", index);
                    jsonGenerator.writeFieldName(t.getSlice(row, index).toString(StandardCharsets.UTF_8));
                }
                else {
                    if (t instanceof RowType) {
                        SingleRowBlock singleRowBlock = (SingleRowBlock) row.getObject(index, Block.class);
                        addNestedRows(jsonGenerator, t, singleRowBlock);
                    }
                    else {
                        createJsonGeneratorWriter(t, false).writeJsonValue(jsonGenerator, row, index);
                    }
                }
                index++;
            }
            jsonGenerator.writeEndObject();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
