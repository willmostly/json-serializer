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
package io.trino.plugin.custom;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.BoundSignature;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.FunctionNullability;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.scalar.ChoicesScalarFunctionImplementation;
import io.trino.operator.scalar.ScalarFunctionImplementation;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.metadata.Signature.withVariadicBound;
import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class JsonSerializeFunction
        extends SqlScalarFunction
{
    private static final String NAME = "json_serialize";
    private static final String DESCRIPTION = "Serialize a row as json";

    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonSerializeFunction.class, NAME, RowType.class, ArrayType.class);

    public JsonSerializeFunction()
    {
        super(new FunctionMetadata(
                Signature.builder()
                        .typeVariableConstraints(withVariadicBound("T", "row"))
                        .typeVariableConstraints(withVariadicBound("V", "array"))
                        .argumentTypes(new TypeSignature("V"), new TypeSignature("T"))
                        .returnType(VARCHAR.getTypeSignature())
                        .build(),
                new FunctionNullability(false, ImmutableList.of(false, false)),
                true,
                true,
                DESCRIPTION,
                SCALAR));
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(BoundSignature boundSignature)
    {
        List<Type> keyTypes = boundSignature.getArgumentType(0).getTypeParameters();
        List<Type> valTypes = boundSignature.getArgumentType(1).getTypeParameters();

        FunctionDependencyDeclaration.FunctionDependencyDeclarationBuilder builder = FunctionDependencyDeclaration.builder();
        for (Type type : valTypes) {
            builder.addType(type.getTypeSignature());
        }
        for (Type type : keyTypes) {
            builder.addType(type.getTypeSignature());
        }
        return builder.build();
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        return new ChoicesScalarFunctionImplementation(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                METHOD_HANDLE);
    }

    @UsedByGeneratedCode
    public static Slice json_serialize(ConnectorSession session, Block names, Block row)
    {
        return null;
    }
}
