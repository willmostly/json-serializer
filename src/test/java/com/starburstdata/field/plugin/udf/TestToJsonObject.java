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

import io.trino.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestToJsonObject
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
            throws Exception
    {
        registerScalar(ToJsonObject.class);
    }

    @Test
    public void testJsonSerialize()
    {
        assertFunction("to_json_object(Row('k1', 2, 'k2', 'banana', 'k3', ARRAY[1,2,3,44]))", VARCHAR, "{\"k1\":2,\"k2\":\"banana\",\"k3\":[1,2,3,44]}");
    }
}
