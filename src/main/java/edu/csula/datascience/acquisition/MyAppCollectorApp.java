package edu.csula.datascience.acquisition;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

public class MyAppCollectorApp {

	private final static String indexName = "bd-business";
	private final static String TypeName = "Crime_Business";
	private final static String localPath = Paths.get("src/main/resources/").toAbsolutePath().toString() + "\\";
	private final static String awsAddress = "http://search-cs594-hw4-k3aklw5b3vkrwuakfxfjr4dnva.us-west-2.es.amazonaws.com";
	
	public static void main(String[] args) throws URISyntaxException {
		
		Node node = nodeBuilder()
				.settings(Settings.builder().put("cluster.name", "CS594").put("path.home", "elasticsearch-data"))
				.node();
		Client client = node.client();

		// create bulk processor
		BulkProcessor bulkProcessor = createBulkProcessor(client);

		MyAppSource source = new MyAppSource();
		MyAppCollector collector = new MyAppCollector();

		String[] Crime15HeaderList = buildCrime15Header();
		String[] Crime04HeaderList = buildCrime04Header();
		String[] BusinessHeaderList = buildBusinessHeader();
		String[] PropertyHeaderList = buildPropertyHeader();

		// Download files from source
		source.Download("https://data.lacounty.gov/api/views/9trm-uz8i/rows.csv?accessType=DOWNLOAD&bom=true",
				"Property");
		source.Download("https://data.lacounty.gov/api/views/3dxh-c6jw/rows.csv?accessType=DOWNLOAD&bom=true",
				"Crime04-15");
		source.Download("https://data.lacounty.gov/api/views/ca5f-5zzs/rows.csv?accessType=DOWNLOAD&bom=true",
				"Crime2015");
		source.Download("https://data.lacity.org/api/views/r4uk-afju/rows.csv?accessType=DOWNLOAD&bom=true",
				"Businesses");

		// Get downloaded files
		File Crime04 = source.getDownloadedFile("Crime04-15");
		File Crime15 = source.getDownloadedFile("Crime2015");
		File Business = source.getDownloadedFile("Businesses");
		File Property = source.getDownloadedFile("Property");

		// Get Filtered data in one file
		File FilteredDataFile = new File(localPath + "FilteredDataFile.csv");

		// Mung & store data into csv files

		if (!FilteredDataFile.exists()) {
			collector.mungeeCrime04(Crime04, Crime04HeaderList, FilteredDataFile);
			System.out.println("FINISHED SAVING FilteredCrimes04.csv");
			
			collector.mungeeCrime15(Crime15, Crime15HeaderList, FilteredDataFile);
			System.out.println("FINISHED SAVING FilteredCrimes15.csv");
			
			collector.mungeeBusiness(Business, BusinessHeaderList, FilteredDataFile);
			System.out.println("FINISHED SAVING FilteredBusiness.csv");
			
			collector.mungeeProperty(Property, PropertyHeaderList, FilteredDataFile);
			System.out.println("FINISHED SAVING FilteredProperty.csv");
		} else {
			System.out.println("FILE " + FilteredDataFile.getName() + " EXISTS");
		}

		// Upload to elasticsearch "HW3"
		//collector.ToElasticSearch(FilteredDataFile,bulkProcessor,indexName,TypeName);

		// Add to AWS "HW4"
		collector.ToAWS(FilteredDataFile,awsAddress,indexName,TypeName);
		
	}

	public static void aggregation(Node node, String indexName, String typeName) {
		SearchResponse sr = node.client().prepareSearch(indexName).setTypes(typeName)
				.setQuery(QueryBuilders.matchAllQuery())
				.addAggregation(AggregationBuilders.terms("Data_type").field("SOURCE").size(Integer.MAX_VALUE)).execute()
				.actionGet();

		// Get your facet results
		Terms agg1 = sr.getAggregations().get("Data_type");

		for (Terms.Bucket bucket : agg1.getBuckets()) {
			System.out.println(bucket.getKey() + ": " + bucket.getDocCount());
		}
	}

	public static BulkProcessor createBulkProcessor(Client client) {
		return BulkProcessor.builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				System.out.println("Facing error while importing data to elastic search");
				failure.printStackTrace();
			}
		}).setBulkActions(10000).setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
				.setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(1)
				.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)).build();
	}

	public static String[] buildCrime15Header() {
		String[] header = { "﻿CRIME_DATE", "STATION_IDENTIFIER", "CRIME_CATEGORY_DESCRIPTION", "STREET", "CITY",
				"LATITUDE", "LONGITUDE", "CRIME_IDENTIFIER" };
		return header;
	}

	public static String[] buildCrime04Header() {
		String[] header = { "﻿CRIME_DATE", "STATION_IDENTIFIER", "CRIME_CATEGORY_DESCRIPTION", "STREET", "CITY",
				"LATITUDE", "LONGITUDE", "ZIP", "CRIME_IDENTIFIER" };
		return header;
	}

	public static String[] buildBusinessHeader() {
		String[] header = { "LOCATION START DATE", "BUSINESS NAME", "STREET ADDRESS", "CITY", "ZIP CODE", "LOCATION",
				"LOCATION ACCOUNT #" };
		return header;
	}

	public static String[] buildPropertyHeader() {
		String[] header = { "RecordingDate", "GeneralUseType", "StreetName", "City", "Units", "ZIPcode5", "CENTER_LAT",
				"CENTER_LON", "AssessorID" };
		return header;
	}

}
