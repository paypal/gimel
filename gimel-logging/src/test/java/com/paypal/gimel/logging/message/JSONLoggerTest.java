/*
 * Copyright 2018 PayPal Inc.
 *
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
 *
 */

package com.paypal.gimel.logging.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;


/**
 */
public class JSONLoggerTest {

    SystemLoggerMocked logger = new SystemLoggerMocked("");

    @Test
    public void testEncodeArgsArray() {
        final String arrayJson = this.logger.toJson(new int[] { 1, 2, 3 });
        assertTrue(arrayJson != null);
        assertEquals(arrayJson, "{\"data\":[1,2,3]}");
    }

    @Test
    public void testEncodeArgsMap() {
        final Map<String, String> data = new HashMap<>();
        data.put("key", "value");
        final String arrayJson = this.logger.toJson(data);
        assertTrue(arrayJson != null);
        assertEquals(arrayJson, "{\"key\":\"value\"}");
    }

}
