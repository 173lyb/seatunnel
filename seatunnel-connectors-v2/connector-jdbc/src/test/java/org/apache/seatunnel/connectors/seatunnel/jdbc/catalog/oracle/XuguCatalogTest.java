/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguCatalog;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

@Disabled("Please Test it in your local environment")
class XuguCatalogTest {
    @Test
    void testCatalog() {
        XuguCatalog catalog =
                new XuguCatalog(
                        "XUGU",
                        "SYSDBA",
                        "SYSDBA",
                        JdbcUrlUtil.getUrlInfo("jdbc:xugu://10.28.23.110:5138/SYSTEM"),
                        null);

        catalog.open();

        List<String> strings = catalog.listDatabases();

        CatalogTable table = catalog.getTable(TablePath.of("SYSTEM", "SYSDBA", "AAA_TEST3"));

        //catalog.createTable(new TablePath("XE", "TEST", "TEST003"), table, false);
    }
}
