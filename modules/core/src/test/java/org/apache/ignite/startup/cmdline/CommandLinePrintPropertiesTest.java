/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.startup.cmdline;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.IgniteSystemProperty;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.lang.reflect.Modifier.isFinal;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static org.apache.ignite.startup.cmdline.CommandLineStartup.PRINT_PROPS_COMMAND;

/**
 * Tests print Ignite system properties.
 */
public class CommandLinePrintPropertiesTest extends GridCommonAbstractTest {
    /** Ignite system property pattern. */
    private final Pattern propPtrn = Pattern.compile("^(\\w+).* +- \\[\\w+](\\[Deprecated]|) (.*)");

    /** @throws Exception If failed. */
    @Test
    public void testPrintProperties() throws Exception {
        Map<String, Field> expProps = new HashMap<>();

        for (Field field : IgniteSystemProperties.class.getFields()) {
            int mod = field.getModifiers();

            if (isPublic(mod) && isStatic(mod) && isFinal(mod) && String.class.equals(field.getType()))
                expProps.put(U.staticField(IgniteSystemProperties.class, field.getName()), field);

            assertTrue("Ignite system property must be annotated by @" +
                IgniteSystemProperty.class.getSimpleName() + " [field=" + field + ']',
                field.isAnnotationPresent(IgniteSystemProperty.class));
        }

        assertFalse(expProps.isEmpty());

        GridJavaProcess proc = GridJavaProcess.exec(
            CommandLineStartup.class,
            PRINT_PROPS_COMMAND,
            log,
            s -> {
                Matcher matcher = propPtrn.matcher(s);

                if (matcher.matches()) {
                    String name = matcher.group(1);
                    boolean deprecated = !matcher.group(2).isEmpty();
                    String desc = matcher.group(3);

                    Field field = expProps.remove(name);

                    assertNotNull("Duplicate property found [name=" + name + ']', field);

                    assertEquals(field.isAnnotationPresent(Deprecated.class), deprecated);

                    assertFalse("Description is empty.", desc.isEmpty());
                }
            },
            null);

        proc.getProcess().waitFor();

        assertEquals(0, proc.getProcess().exitValue());

        assertTrue("Not all properties printed: " + expProps, expProps.isEmpty());
    }
}
