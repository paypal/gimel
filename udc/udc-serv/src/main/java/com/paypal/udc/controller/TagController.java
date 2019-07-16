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

package com.paypal.udc.controller;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.google.gson.Gson;
import com.paypal.udc.entity.integration.common.DatasetTagMapInput;
import com.paypal.udc.entity.integration.common.Tag;
import com.paypal.udc.exception.ValidationError;
import com.paypal.udc.service.ITagService;
import com.paypal.udc.util.enumeration.UserAttributeEnumeration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;


@RestController
@RequestMapping("tag")
@Api(value = "Tag Services", description = "Operations pertaining to Tag Service")
public class TagController {

    final static Logger logger = LoggerFactory.getLogger(TagController.class);

    final Gson gson = new Gson();

    private ITagService tagService;
    private HttpServletRequest request;
    private String userType;

    @Autowired
    private TagController(final ITagService tagService, final HttpServletRequest request) {
        this.tagService = tagService;
        this.request = request;
        this.userType = UserAttributeEnumeration.SUCCESS.getFlag();
    }

    @ApiOperation(value = "View the tag based on ID", response = Tag.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Tag"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("tag/{id}")
    public ResponseEntity<?> getTagById(@PathVariable("id") final Long id) {

		try {
			final Tag tag = this.tagService.getTagById(id);
			return new ResponseEntity<Tag>(tag, HttpStatus.OK);
		} catch (final ValidationError e) {
			return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
		}

	}

    @ApiOperation(value = "View the tag based on provider ID", response = Iterator.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
	@GetMapping("tagByProvider/{id}")
	public ResponseEntity<?> getTagByProviderId(@PathVariable("id") final Long id) {

		final List<Tag> tags = this.tagService.getByProviderId(id);
		return new ResponseEntity<List<Tag>>(tags, HttpStatus.OK);
	}

    @ApiOperation(value = "View the tags based on list of ids", response = Iterator.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("tags/{ids}")
	public ResponseEntity<?> getTags(@PathVariable("ids") final String ids) {
		final List<Tag> tags = this.tagService.getByTagIds(ids);
		return new ResponseEntity<List<Tag>>(tags, HttpStatus.OK);
	}

    @ApiOperation(value = "View the Tag based on Name", response = Tag.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved Tag"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("tagByName/{name:.+}")
	public ResponseEntity<?> getTagByName(@PathVariable("name") final String name) {
		final Tag tag = this.tagService.getTagByName(name);
		return new ResponseEntity<Tag>(tag, HttpStatus.OK);
	}

    @ApiOperation(value = "View a list of available tags", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved list"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("tags")
	public ResponseEntity<?> getAllTags() {
		final List<Tag> list = this.tagService.getAllTags();
		return new ResponseEntity<List<Tag>>(list, HttpStatus.OK);
	}

    @ApiOperation(value = "Insert a Tag for a Dataset", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Tag for a dataset"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("tagForDataset")
    public ResponseEntity<?> addTagForDataset(@RequestBody final DatasetTagMapInput datasetTagMapInput) {
        try {
            this.tagService.addTagForDataset(datasetTagMapInput);
            return new ResponseEntity<String>("Successfully inserted dataset tag", HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
        catch (IOException | InterruptedException | ExecutionException e) {
            return new ResponseEntity<String>(this.gson.toJson(e), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Insert a Tag", response = Tag.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully inserted Tag"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("tag")
    public ResponseEntity<?> addTag(@RequestBody final Tag tag) {
        Tag insertedTag;
        try {
            insertedTag = this.tagService.addTag(tag);
            return new ResponseEntity<Tag>(insertedTag, HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "Update an tag based on Input", response = String.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated tag"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("tag")
    public ResponseEntity<String> updateTag(@RequestBody final Tag tag) {
        Tag updatedTag;
        try {
            updatedTag = this.tagService.updateTag(tag);
            return new ResponseEntity<String>(this.gson.toJson(updatedTag), HttpStatus.OK);
        }
        catch (final ValidationError e) {
            return new ResponseEntity<String>(this.gson.toJson(e), e.getErrorCode());
        }
    }

    @ApiOperation(value = "View the tags based on dataset ID", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved SchemaDatasetMap"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("tagsByDataset/{id}")
	public ResponseEntity<?> getTagsById(@PathVariable("id") final Long id) {
		final List<Tag> tags = this.tagService.getTagsByDatasetId(id);
		return new ResponseEntity<List<Tag>>(tags, HttpStatus.OK);
	}
}
