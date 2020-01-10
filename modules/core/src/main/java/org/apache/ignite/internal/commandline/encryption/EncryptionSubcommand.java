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

package org.apache.ignite.internal.commandline.encryption;

/**
 * Set of encryption subcommands.
 *
 * @see EncryptionCommand
 */
public enum EncryptionSubcommand {
    /** Get current master key subcommand. */
    GET_MASTER_KEY("get_master_key"),

    /** Change master key subcommand. */
    CHANGE_MASTER_KEY("change_master_key");

    /** Encryption subcommand name. */
    private final String name;

    /** @param name Encryption subcommand name. */
    EncryptionSubcommand(String name) {
        this.name = name;
    }

    /**
     * @param text Command text.
     * @return Command for the text.
     */
    public static EncryptionSubcommand of(String text) {
        for (EncryptionSubcommand cmd : EncryptionSubcommand.values()) {
            if (cmd.name.equalsIgnoreCase(text))
                return cmd;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
