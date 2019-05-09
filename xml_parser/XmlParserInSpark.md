#Flattening Nested Many-to-Many Relationships in XML Files

##Introduction
In most of the 2000's, XML was a widely used semi-structured format for data storage. It has since declined in popularity to other formats such as JSON but there are use cases for parsing XML today far more often than one might think.
The most obvious use case is for processing historical XML files (yes! 15-20 year old data) but there are still many systems that output XML files for logs among other things.

The use case covered in this document is for compressed (bz2) XML formatted files. Specifically, these files are [NAXML](http://xml.coverpages.org/naxml.html) which was a common format for transaction logs coming from convenient store or gas station registers. 
The hardware that produces these logs may not be upgraded for decades such as the pumps at a gas station; hence, it is still common to come across newly generated NAXML files and parsing them into a structured optimal format like Parquet is the first step for success for most intended use cases of the data down the line. 
Despite XML being more complicated to parse than other formats such as JSON, there is a chance you or your business will have to parse historic NAXML files or new log files from old hardware producing NAXML files.
This document serves to present a special use case of parsing such XML files with nested transactional data.

First, the raw data is described to show the various nuances of the data structure. 
Second, the difficulties in processing the data in a distributed system is laid out.
Finally, the implemented solution is presented and limitations are discussed.

###Raw Data Files
A typical file is structured in the following nested object structure:

	<NAXML-POSJournal>
		<TransmissionHeader>
			<TransmissionID>1234567890</TransmissionID>
			<Date>2018-10-30</Date>
			<Time>13:26:05</Time>
			<Status>OK</Status>
			<TransmissionDetails>
				<tag_type_1>...<tag_type_1>
				<tag_type_2>...<tag_type_2>
				<tag_type_1>...<tag_type_1>
						...
			</TransmissionDetails>
		</TransmissionHeader >
		<JournalReport>
			<ReportHeader>
				... Header Details and Nested Structure
			</ReportHeader>
			<SaleEvent>
				<TransactionID>X123456789</TransactionID>
				<DetailGroup>
					<LineItem>
						<Tender></Tender>
					</LineItem>
					<LineItem>
						<Customer></Customer>
					</LineItem>
				</DetailGroup>
			</SaleEvent>
			<SaleEvent>
				<TransactionID>X987654321</TransactionID>
				<DetailGroup>
					<LineItem>...</LineItem>
					<LineItem>...</LineItem>
				</DetailGroup>
			</SaleEvent>
			<FinancialEvent>
				<DetailGroup>
					<LineItem>
						<Account>...</Account>
					</LineItem>
					<LineItem>
						<Customer>...</Customer>
					</LineItem>
				</DetailGroup>
			</FinancialEvent>
		</JournalReport >
	</NAXML-POSJournal>

Each file has a single main tag; in this case the `NAXML-POSJournal` tag. Each file is produced by a single piece of hardware for a specific time duration. This information is contained in the `TransmissionHeader` tag.

Further, all of the transactional data is contained within the `JournalReport` tag. Each transaction event is an objectin the `JournalReport` and can be one of multiple possible tags such as `SaleEvent` or `FinancialEvent` as well as many more. Any number of each type of event within the parent `JournalReport` is acceptable.
Similarly, each one of the event tags may have any nested objects structure inside with no consistency across different tags. 

For example, the `JournalReport` is equivalent to an Array[Array([Strings, String])] where each instance of the internal Array[String, String] is of random sizes and keys.

To demonstrate this point further, both `SaleEvent` and `FinancialEvent` have a `DetailGroup` tag which is an array of `LineItem` but the contained line items have different tags nested inside. This nested pattern is present across the entire file with an unknown depth.

Looking at such a file can quickly feel like a bird's nest in circuits or spagetti code. If the file is accompanied by an XSD, it is much easier to comprehend both the structure and give much insight into the proposed solution.

An XSD file describes the nested structure of the data and can be used to determine object hierarchy which is the inspiration of this proposed solution; to flatten the data into the way the XSD file is flattened. More on this soon!

###The Distributed Problem
Before discussion of the problems in distributed processing of these NAXML files, it is important to note that there are many XML parsing libraries in all modern langauges to process such files. The problem becomes of how to process thousands or millions of these files in an efficient manner using a Spark cluster. 

At first glance, it might seem trivial to parse this file using Spark. After all, Spark has a native XML parser and one could create the underlying *schema* using StructType, MapType and ArrayType to match the nested objects inside every file. A few attempts at this and it becomes clear that it is impossible to define a schema that allows us to capture all the nested objects since the depth is unknown. *Limitations of the Spark XML Parser* as discussed below.

Moving away from the actual parsing of the XML, there is also the problem of reading of the file content by a Spark cluster.
To parse the content of the file correctly, every opening tag must be matched with a closing tag to ensure a properly formatted XML file. This means that a file cannot be read partially by any one executor in the cluster.

This problem is much more complicated to solve and the proposed solution does not present a viable option for partially reading in the XML file; hence allowing a file to be read partially by multiple executors.

###Limitations of the [Spark XML Parser](https://github.com/databricks/spark-xml)
Even if a schema to accomodate the nested structure could be constructed, the existing Spark XML parser is unable to process this file.
The Spark XML Parser is very simple and designed for a very narrow use case. In short, it expects a consistent and repeated structure that it can parse into rows of StructType.
For example, a root tag, which could be `ReportJournal`, with potentially the row tag as each row of events but since the events don't have a consistent `rowTag` this option does little to return a dataframe of rows.
It would be ideal to be able to define a rowTag as an interface of *EventType* where `SalesEvent` and `FinancialEvent` all implement the interface.
The only way this would work (which it still wouldn't) is if the schema of `rowTag` was a very large flattened structure of all the various unique "keys" in all implementations of the *EventType* interface so the output row is a StructType that contains every possible key of all objects.

##The Solution
As alluded to earlier, the solution uses the structure of the XSD to create a complete map of every file's content flattened. Using a relational database model approach, every distinct "tag" type is defined as a table, every instance of the tag type is inserted as a row and given a unique key as well as a foreign key as to which tag it was nested in.
As the XML structure is traversed recursively, the parent-child relationship is clear.
The result is a series of dataframes, one for each tag type representing a "table" of rows.

It's important to note that there is an obscure distinction here, the "table" concept is defined for every distinct tag is that NOT a leaf node. Simply put, if a tag does not nest any tags inside of it, it is assumed that either the tag's inner text or attributes are desired as columns of the parent tag.

For example:

	<user inSchool="True">
		<name>Billie</name>
		<age>10</age>
		<hobbies>
			<hobbie>fishing</hobbie>
			<hobbie>hunting treasure</hobbie>
			<hobbie>training to be a pirate</hobbie>
		</hobbies>
	</user>

In the example above, both the tags `user` and `hobbies` would become a distinct dataframe while `inSchool`, `name`, `age`, and `hobbie` would be column names in the parent tag's dataframe. The above example would result in the dataframes below:

	>>users_df.collect()
	|--------------- user ---------------|
	|  PK  |  inSchool  |  name  |  age  |
	|------|------------|--------|-------|
	|1     |True        |Billie  |10     |
	|------------------------------------|

	>>hobbies_df.collect()
	|---------------------- hobbies ---------------------|
	|  PK  |  FK  |  FK_Object  | hobbies                |
	|------|------|-------------|------------------------|
	|1     |1     |user         |fishing                 |
	|2     |1     |user         |hunting treasure        |
	|3     |1     |user         |training to be a pirate |
	|----------------------------------------------------|

Finally, there is no guarantee that every tag of type `user` will have the attribute `inSchool` defined at all. In order to create consistency, it is expected that all attributes and tags to be processed and recorded must be defined as a type of schema of sorts.

In the simple example above, a data dictionary such as below must be defined.

	schema = {
		"user": [
			"inSchool",
			"name",
			"age"
		],
		"hobbies": [
			"hobbies",
			"a_non_existing_tag"
		]
	}

With the above dictionary, every key is expected to be a table/dataframe with a primary key and every list is the columns for that table. 
If something is ommited, such as name or inSchool, it is left out of the final table. Alternatively, if a field is defined that does not exist, such as 'a_non_existing_tag', it is still defined as a column with the same name in the final table and filled in with `Null` values.

This logic ensures that all rows of the table "hobbies" or "user" are consistent in schema. The current solution expects this dictionary to be pre-defined. If an XSD is given, this dictionary can be automatically created from parsing the XSD; however, this is out of scope for this document.

###The implementation
The implementation follows the logic below:

1. Each file is read as a wholeTextFile.
  Yes this is important so we can parse the file at once to ensure it is not curropt.
  All tags in the file must be given a foreign key matching the outer most parent tag.

1. Each child is traversed recursively starting at the root.

1. A primary key is generated for the node

1. For every node, the attributes are defined as columns for itself and inserted as one row.

1. For every node, the node's tag name is defined as a column in the parent and the text content is inserted as a row.

1. The parent's primary key is saved for the node as a foreign key

####Parsing Logic
	from pyspark.sql import functions as f
	parser = XMLParser(data_dictionary)
	file_rdd = spark.sparkContext.wholeTextFiles(directory).map(parser.parseAll)
	spark
		.createDataFrame(file_rdd)
		.write
		.format("parquet")
		.mode("overwrite")
		.option("compression", "snappy")
		.save(file_location)
	type_dfs = {}
	for dataType in data_dictionary:
	  tmp_df = spark.read.parquet(file_location).select(f.explode(dataType).alias("tmp"))
	  exploded_cols = ["tmp.{}".format(col) for col in tmp_df.select(f.explode("tmp")).select("key").distinct().rdd.flatMap(lambda x: x).collect()]
	  type_dfs[dataType] = tmp_df.select(exploded_cols).alias(dataType)

An RDD of the directory of XML files is read and eery file is parsed resulting in an RDD of one row per file; this ensures an entire file is processed at once on one executor. If that row is lost, it must be calculated all over again ensuring that the foreign key to primary key relationship of the nested tags is respected.

The each row of this RDD has a Row() object with a key for file name, and a key for every tag type parsed as per the data dictionary supplied. Using the "user" example above, the Row() object has the keys `user` and `hobbies` and both user and hobbies are an array of dictionaries.

This RDD is converted to a dataframe and saved as is. This is basically a checkpoint. If there is a loss of data on write, an entire row would have to be recalculated resulting in consistent primary and foreign keys as expressed earlier.

The second step is to re-read the data and explode each of the data types into it's own dataframe. Since one file may contain 1 user and another file 5 users, the explode returns a "user" dataframe with 6 rows.

The final output is a dictionary of dataframes.

####The Parsing Object
	import xml.etree.ElementTree as ET
	from pyspark.sql import Row
	from pyspark.sql.types import *
	import datetime
	import random
	import hashlib
	class XMLParser():
		def __init__(self, data_dictionary):
			self._xml_data_dictionary = data_dictionary
	  
		def parseAll(self, file):
		    timestamp = datetime.datetime.now()
		    file_name = file[0]
		    xml_string = file[1].encode('utf-8')
		    root = ET.fromstring(xml_string)
		    output = {}
		    
		    def processChild(elem, parent_guid, parent_tag):
		      	elem_tag = remove_tag_namespace(elem.tag)
	            generated_elem_guid = get_guid(timestamp, file_name, elem.tag)
	            elem_data = {
	                "PK_{}".format(elem_tag): generated_elem_guid,
	                "FK_OBJECT": parent_tag,
	                "FK_ID": parent_guid,
	                "FILE_NAME": file_name,
	                "PROCESS_TS": str(timestamp),
	                "CURROPT_RECORD": ""
	            }
	            # Filling in the attributes from the element
	            # NOTE: This loop does the job of filling in None for all the values we care about
	            for potential_attribute in self._xml_data_dictionary[elem_tag]:
	                elem_data[potential_attribute] = elem.get(potential_attribute, None)
	            for child in list(elem):
	                child_tag = remove_tag_namespace(child.tag)
	                if ((len(child) == 0) and
	                        (elem_tag in self._xml_data_dictionary) and
	                        (child_tag in self._xml_data_dictionary[elem_tag])):
	                    elem_data[child_tag] = child.text
	                    continue
	                process_child(child, generated_elem_guid, elem_tag)
	            if elem_tag not in self._xml_data_dictionary:
	                return
	            # We shouldn't need this fill_missing_cols anymore
	            # but leaving in for further testing
	            ready_data = fill_missing_cols(elem_data, elem_tag)
	            output[elem_tag].append(ready_data)
		    
		    def fill_missing_cols(data, type_name):
		      	for required_col in self.config[type_name]:
		      	  if required_col not in data:
		       	   data[required_col] = "Null"
		      	return data
		    
		    def remove_tag_namespace(tag_string):
		      	if '}' not in tag_string:
		       	 return tag_string
		      	return tag_string.split('}', 1)[1]
		    
		    def get_guid(a,b,c):
		      	val_string = "{}_{}_{}_{}".format(a, b, c, random.randint(1,999999999))
		      	return hashlib.sha224(val_string).hexdigest()
		    
			processChild(root, None, None)
		    return Row(**output)

The parsing is done using the native Python XML library. It is worthy to note that Python identifies the library with security vulnerabilities such as injection attacks. The entire file string is parsed into a traversable XML object immediately. Starting at the root node, all nodes are traversed recursively.

The function `processChild` starts at the given node, create's a dictionary for the current node (which becomes a row in the node's respective table), fills in the keys and values as per the defined schema and continues to perform a depth first recursion.

The namespace of the XML tags are removed as namespacing creates often difficult to read large strings; however, it is noted that if namespacing is needed, the `remove_tag_namespace` funtion can be bypassed.

####The GUID Generation
The primary key is defined as a sha224 hash using the timestamp the executor began processing the file, the filename, the node's tagname, and a randomly generated number.
The idea here is that if two executors process the same file, at the exact same moment, then each tag would still get a unique key based on the random number generated. There is a chance for collision but it is assumed to be acceptable.
Given multiple executors can process the same file for redundancy, it is expected that this wouldn't happen at the exact same timestamp and in the case that it does, the machines random number generators would not give the same number.
This is a topic for future improvement.

###Limitations
The biggest limitation of this solution is the requirement to read `wholeTextFiles`. Since XML can have a great deal of repetition, compressed files on disk do not represent an accurate size of the actual file in memory. This can lead to high memory requirements and make the process prone to failure due to memory limitations if files are inconsistent in content/size.

Second, given a file must be processed by one executor in one pass to ensure consistent primary and foreign keys, the first step does not achieve true parallelism. The best performance would come from having enough executors as a factor of the number of files (10000 files with 100 executors?). Performance when 100 very large files are processed by 100 executors cannot be increased by scaling the cluster.

###Future upgrades

####Using the XSD
As mentioned, the data dictionary supplied to the parser object can be defined and created based on an XSD file.

####Very Large Individual Files
There are alternative XML parsing libraries in Python that allow for parsing of XML in chuncks instead of having to read the entire file into memory.
This can be taken advantage of for parsing very large files across multiple executors alleviating the requirement to read wholeTextFiles. In this case, an alternative GUID generation logic should be used, perhaps, using only filenames and ensuring all filenames are unique witin a batch.

##Conclusion
This XML parser is an alternative approach to Spark's native XML parser given its limitations. 
There are equal but different limitations with this approach.
The intent here is to suggest the methodology, logic, and thinking when shifting to the paradigm of parallel processing with a concrete example and a prototype.