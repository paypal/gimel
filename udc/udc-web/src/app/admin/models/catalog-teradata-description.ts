/*
 * Copyright 2019 PayPal Inc.
 *
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

export class TeradataDescription {

  public schemaDatasetColumnMapId: number;
  public schemaDatasetMapId: number;
  public columnCode: string;
  public columnName: string;
  public columnDatatype: string;
  public columnComment: string;
  public createdUser: string;
  public updatedUser: string;

  constructor(schemaDatasetMapId: number, columnCode: string, columnName: string, columnDatatype: string, columnComment: string, createdUser: string, updatedUser: string) {
    this.schemaDatasetMapId = schemaDatasetMapId;
    this.columnCode = columnCode;
    this.columnName = columnName;
    this.columnDatatype = columnDatatype;
    this.columnComment = columnComment;
    this.createdUser = createdUser;
    this.updatedUser = updatedUser;
  }
}
