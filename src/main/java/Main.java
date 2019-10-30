import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.SearchHit;
import org.json.XML;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathExpressionException;
import java.io.*;
import java.util.*;

public class Main {
    public static int counter = 0;
    private static int fileCount = 1;
    private static int JSONcounter = 1;
    private static int txtCounter = 1;
    private static int queryCount;
    private static RestHighLevelClient restClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
    private static ArrayList<Listing> results;

    public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException, TransformerException, ParseException {
        FileWriter resultsWriter;
        File directory;
        File[] files;

        CreateIndex("phase1");
        CreateIndex("phase2");

        File jsonDir = new File("JSON");
        File[] jsonFiles = jsonDir.listFiles();
        directory = new File("phase1_collection");
        files = directory.listFiles();
        fileCount = files.length;
        if (jsonFiles.length == fileCount) {
            for (File file : jsonFiles) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    PutDocuments(file);
                }
            }
        } else {
            for (File file : files) {
                if (file.isFile() && file.getName().endsWith(".xml")) {
                    System.out.println("Modifying file: " + file.getName());
                    Phase1Files(file);
                }
            }
        }

        File collection_2_dir = new File("phase2_collection");
        File[] collection_2 = collection_2_dir.listFiles();
        fileCount = collection_2.length;
        for (File file : collection_2) {
            if (file.isFile() && file.getName().endsWith(".txt")) {
                Phase2Files(file);
            }
        }

        results = new ArrayList<>();
        InputStream in = new FileInputStream("queries/testingQueries.txt");
        BufferedReader buf = new BufferedReader(new InputStreamReader(in));
        String line = buf.readLine();
        List<String> stringList = new ArrayList<>();
        queryCount = 1;
        while (line != null) {
            line = line.replaceAll("[^a-zA-Z0-9 ]", " ");
            stringList.add(line);
            line = buf.readLine();
        }
        for (String s : stringList) {
            Search(s, "phase1");
            queryCount++;
        }

        results.sort(Comparator.comparing(Listing::getQ_id));
        resultsWriter = new FileWriter("Output/phase_1_results.txt");
        for (Listing listing : results) {
            resultsWriter.write(listing.toString() + "\n");
        }
        resultsWriter.close();

        File queryDirectory = new File("new_queries");
        File[] new_queries = queryDirectory.listFiles();
        List<String> queryList = new ArrayList<>();
        for (File file : new_queries) {
            if (file.isFile() && file.getName().endsWith(".txt")) {
                buf = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                String contents = "";
                line = buf.readLine();
                while (line != null) {
                    contents += line.replaceAll("[^a-zA-Z0-9 ]", " ");
                    line = buf.readLine();
                }
                queryList.add(contents);
            }
        }

        queryCount = 1;
        results = new ArrayList<>();
        for (String query : queryList) {
            Search(query, "phase1");
            queryCount++;
        }

        resultsWriter = new FileWriter("Output/phase_1_new_queries_results.txt");
        for (Listing listing : results) {
            resultsWriter.write(listing.toString() + "\n");
        }
        resultsWriter.close();

        queryCount = 1;
        results = new ArrayList<>();
        for (String query : queryList) {
            Search(query, "phase2");
            queryCount++;
        }


        resultsWriter = new FileWriter("Output/phase_2_new_queries_results.txt");
        for (Listing listing : results) {
            resultsWriter.write(listing.toString() + "\n");
        }
        resultsWriter.close();

        // MLT query with default values
        queryCount = 1;
        results = new ArrayList<>();
        for (String s : stringList) {
            Search(s, "phase2");
            queryCount++;
        }

        results.sort(Comparator.comparing(Listing::getQ_id));
        resultsWriter = new FileWriter("Output/phase_2_default_mlt_results.txt");
        for (Listing listing : results) {
            resultsWriter.write(listing.toString() + "\n");
        }
        resultsWriter.close();

        //MLT Query Test 1
        queryCount = 1;
        results = new ArrayList<>();
        for (String s : stringList) {
            MLTQuery(s, "phase2", 1, 15, 6, 10000, 5);
            queryCount++;
        }

        results.sort(Comparator.comparing(Listing::getQ_id));
        resultsWriter = new FileWriter("Output/phase_2_mlt_results_test_1.txt");
        for (Listing listing : results) {
            resultsWriter.write(listing.toString() + "\n");
        }
        resultsWriter.close();

        //MLT Query Test 2
        queryCount = 1;
        results = new ArrayList<>();
        for (String s : stringList) {
            MLTQuery(s, "phase2", 3, 31, 40, 50, 10);
            queryCount++;
        }

        results.sort(Comparator.comparing(Listing::getQ_id));
        resultsWriter = new FileWriter("Output/phase_2_mlt_results_test_2.txt");
        for (Listing listing : results) {
            resultsWriter.write(listing.toString() + "\n");
        }
        resultsWriter.close();

        //MLT Query Test 3
        queryCount = 1;
        results = new ArrayList<>();
        for (String s : stringList) {
            MLTQuery(s, "phase2", 5, 100, 3, 1000, 31);
            queryCount++;
        }

        results.sort(Comparator.comparing(Listing::getQ_id));
        resultsWriter = new FileWriter("Output/phase_2_mlt_results_test_3.txt");
        for (Listing listing : results) {
            resultsWriter.write(listing.toString() + "\n");
        }
        resultsWriter.close();
        restClient.close();
    }

    private static void MLTQuery(String parameter, String indexName, int min_term_freq, int max_query_terms,
                                 int min_doc_freq, int max_doc_freq, int minimum_should_match) throws IOException, ParseException {
        GetIndexRequest request = new GetIndexRequest(indexName);
        boolean exists = restClient.indices().exists(request, RequestOptions.DEFAULT);
        if (!exists) {
            System.out.println("No index with that name found.");
        } else {
            SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest();
            searchTemplateRequest.setRequest(new SearchRequest("phase2"));
            searchTemplateRequest.setScriptType(ScriptType.INLINE);

            searchTemplateRequest.setScript(
                    "{" +
                            "\"size\": 21," +
                            "\"_source\": \"text\","+
                            "  \"query\": {" +
                            "    \"more_like_this\": {" +
                            "      \"fields\": [" +
                            "        \"text\"" +
                            "      ]," +
                            "      \"like\": \"{{param_like}}\"," +
                            "      \"min_term_freq\" : \"{{param_min_term_freq}}\"," +
                            "      \"min_doc_freq\" : \"{{param_min_doc_freq}}\"," +
                            "      \"max_query_terms\" : \"{{param_max_query_freq}}\"," +
                            "      \"max_doc_freq\" : \"{{param_max_doc_freq}}\"," +
                            "      \"minimum_should_match\" : \"{{param_minimum_should_match}}\"" +
                            "    }" +
                            "  }" +
                            "}"
            );

            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("param_like", parameter);
            scriptParams.put("param_min_term_freq", String.valueOf(min_term_freq));
            scriptParams.put("param_min_doc_freq", String.valueOf(min_doc_freq));
            scriptParams.put("param_max_query_freq", String.valueOf(max_query_terms));
            scriptParams.put("param_max_doc_freq", String.valueOf(max_doc_freq));
            scriptParams.put("param_minimum_should_match", minimum_should_match+"%");
            searchTemplateRequest.setScriptParams(scriptParams);
            searchTemplateRequest.setExplain(true);
            SearchTemplateResponse searchTemplateResponse = restClient.searchTemplate(searchTemplateRequest, RequestOptions.DEFAULT);
            SearchResponse searchResponse = searchTemplateResponse.getResponse();
            int hitCount = 0;
            for (SearchHit hit : searchResponse.getHits()) {
                if (hitCount > 0) {
                    if (queryCount == 10) {
                        results.add(new Listing("Q" + 10, "Q0", hit.getId(), 0, hit.getScore(), "MLTQuery"));
                    } else {
                        results.add(new Listing("Q0" + queryCount, "Q0", hit.getId(), 0, hit.getScore(), "MLTQuery"));
                    }
                    hitCount++;
                } else
                    hitCount++;
            }
        }
    }

    private static void CreateIndex(String indexName) throws IOException, ParseException {
        GetIndexRequest request = new GetIndexRequest(indexName);
        boolean exists = restClient.indices().exists(request, RequestOptions.DEFAULT);
        if (exists) {
            System.out.println("Index already exists.");
        } else {
            System.out.println("Creating index " + indexName);
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(new FileReader("Indices/index.json"));
            createIndexRequest.source(json.toJSONString(), XContentType.JSON);
            CreateIndexResponse response = restClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            System.out.println("Response:\n\tAcknowledged: " + response.isAcknowledged() + "\n\tShards Acknowledged: " + response.isShardsAcknowledged());
        }
    }

    private static void PutDocuments(File file) throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        Object object = parser.parse(new FileReader(file));
        JSONObject ele = (JSONObject) object;
        JSONObject obj = (JSONObject) ele.get("project");
        String identifier = (String) obj.get("identifier");
        Long rcn = (Long) obj.get("rcn");
        String xmlns = (String) obj.get("xmlns");
        Object acronym = obj.get("acronym");
        String text = (String) obj.get("text");

        IndexRequest indexRequest = new IndexRequest("phase1")
                .id(Long.toString(rcn))
                .source("identifier", identifier, "xmlns", xmlns, "rcn", rcn, "acronym", acronym, "text", text);

        IndexResponse response = restClient.index(indexRequest, RequestOptions.DEFAULT);

        System.out.println("\nFiles Uploaded: " + JSONcounter + "/" + 18316);
        JSONcounter++;
    }

    private static void PutDocuments(String filePath) throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        Object object = parser.parse(new FileReader(filePath));
        JSONObject ele = (JSONObject) object;
        JSONObject obj = (JSONObject) ele.get("project");
        String identifier = (String) obj.get("identifier");
        int rcn = (int) obj.get("rcn");
        String xmlns = (String) obj.get("xmlns");
        Object acronym = obj.get("acronym");
        String text = (String) obj.get("text");

        IndexRequest indexRequest = new IndexRequest("phase1")
                .id(Integer.toString(rcn))
                .source("identifier", identifier, "xmlns", xmlns, "rcn", rcn, "acronym", acronym, "text", text);

        IndexResponse response = restClient.index(indexRequest, RequestOptions.DEFAULT);

        System.out.println("\nFiles Modified: " + JSONcounter + "/" + 18316);
        JSONcounter++;
    }

    private static void Search(String parameter, String indexName) throws IOException {
        GetIndexRequest request = new GetIndexRequest(indexName);
        boolean exists = restClient.indices().exists(request, RequestOptions.DEFAULT);
        if (!exists) {
            System.out.println("No index with that name found.");
        } else {
            SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest();
            searchTemplateRequest.setRequest(new SearchRequest(indexName));
            searchTemplateRequest.setScriptType(ScriptType.INLINE);

            searchTemplateRequest.setScript(
                    "{" +
                            "\"size\": 21," +
                            "  \"query\": {" +
                            "    \"more_like_this\": {" +
                            "      \"fields\": [" +
                            "        \"text\"" +
                            "      ]," +
                            "      \"like\": \"{{param_like}}\"" +
                            "    }" +
                            "  }" +
                            "}"
            );

            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("param_like", parameter);
            searchTemplateRequest.setScriptParams(scriptParams);
            searchTemplateRequest.setExplain(true);
            SearchTemplateResponse searchTemplateResponse = restClient.searchTemplate(searchTemplateRequest, RequestOptions.DEFAULT);
            SearchResponse searchResponse = searchTemplateResponse.getResponse();
            int hitCount = 0;
            for (SearchHit hit : searchResponse.getHits()) {
                if (hitCount > 0) {
                    if (queryCount == 10) {
                        results.add(new Listing("Q" + 10, "Q0", hit.getId(), 0, hit.getScore(), "Phase1Search"));
                    } else {
                        results.add(new Listing("Q0" + queryCount, "Q0", hit.getId(), 0, hit.getScore(), "Phase1Search"));
                    }
                    hitCount++;
                } else
                    hitCount++;
            }
        }
    }

    /*private static void Phase2Search(String parameter, String indexName) throws IOException {
        GetIndexRequest request = new GetIndexRequest(indexName);
        boolean exists = restClient.indices().exists(request, RequestOptions.DEFAULT);
        if (!exists) {
            System.out.println("No index with that name found.");
        } else {
            SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest();
            searchTemplateRequest.setRequest(new SearchRequest("phase2"));
            searchTemplateRequest.setScriptType(ScriptType.INLINE);

            searchTemplateRequest.setScript(
                    "{" +
                            "\"size\": 21," +
                            "  \"query\": {" +
                            "    \"more_like_this\": {" +
                            "      \"fields\": [" +
                            "        \"text\"" +
                            "      ]," +
                            "      \"like\": \"{{param_like}}\"" +
                            "    }" +
                            "  }" +
                            "}"
            );

            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("param_like", parameter);
            searchTemplateRequest.setScriptParams(scriptParams);
            searchTemplateRequest.setExplain(true);
            SearchTemplateResponse searchTemplateResponse = restClient.searchTemplate(searchTemplateRequest, RequestOptions.DEFAULT);
            SearchResponse searchResponse = searchTemplateResponse.getResponse();
            int hitCount = 0;
            for (SearchHit hit : searchResponse.getHits()) {
                if (hitCount > 0) {
                    if (queryCount == 10) {
                        results.add(new Listing("Q" + 10, "Q0", hit.getId(), 0, hit.getScore(), "Phase2Search"));
                    } else {
                        results.add(new Listing("Q0" + queryCount, "Q0", hit.getId(), 0, hit.getScore(), "Phase2Search"));
                    }
                    hitCount++;
                } else
                    hitCount++;
            }
        }
    }
*/
    public static void Phase1Files(File xmlFile) throws ParserConfigurationException, IOException, SAXException, TransformerException, ParseException {
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

        Document doc = dBuilder.parse(xmlFile);
        Element rootElement = doc.getDocumentElement();


        NodeList nodes = doc.getElementsByTagName("project");

        for (int i = 0; i < nodes.getLength(); i++) {
            Node ni = nodes.item(i);

            if (ni.getNodeType() == Node.ELEMENT_NODE) {
                counter++;
                Element ei = (Element) ni; //type cast to element

                String rcn = ei.getElementsByTagName("rcn").item(0).getTextContent();
                String acronym = ei.getElementsByTagName("acronym").item(0).getTextContent();
                String objective = ei.getElementsByTagName("objective").item(0).getTextContent();
                String title = ei.getElementsByTagName("title").item(0).getTextContent();
                String identifier = ei.getElementsByTagName("identifier").item(0).getTextContent();

                CreateNewXML(xmlFile.getName(), rootElement.getNodeName(), rootElement.getAttribute("xmlns"), rcn, acronym, objective, title, identifier);
            }
        }

        System.out.println("\nFiles Modified: " + counter + "/" + fileCount);
    }

    private static void Phase2Files(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String content = "", line = reader.readLine();
        while (line != null) {
            content += line;
            line = reader.readLine();
        }

        IndexRequest indexRequest = new IndexRequest("phase2")
                .id(file.getName().replace(".txt", ""))
                .source("text", content);
        IndexResponse response = restClient.index(indexRequest, RequestOptions.DEFAULT);

        System.out.println("\nFile Uploaded: " + txtCounter + "/ 18316");

        txtCounter++;
    }

    public static void CreateNewXML(String fileName, String rootElement, String xmlnsValue, String rcnValue, String acronymValue,
                                    String objectiveValue, String titleValue, String identifierValue) throws ParserConfigurationException, TransformerException, IOException, ParseException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = factory.newDocumentBuilder();
        Document doc = documentBuilder.newDocument();

        Element root = doc.createElement(rootElement);
        root.setAttribute("xmlns", xmlnsValue);
        doc.appendChild(root);

        Element rcn = doc.createElement("rcn");
        rcn.setTextContent(rcnValue);
        root.appendChild(rcn);

        Element acronym = doc.createElement("acronym");
        acronym.setTextContent(acronymValue);
        root.appendChild(acronym);

        Element text = doc.createElement("text");
        text.setTextContent(titleValue + ": " + objectiveValue);
        root.appendChild(text);

        Element identifier = doc.createElement("identifier");
        identifier.setTextContent(identifierValue);
        root.appendChild(identifier);

        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();

        DOMSource src = new DOMSource(doc);
        StringWriter writer = new StringWriter();

        transformer.transform(src, new StreamResult(writer));

        org.json.JSONObject jsonOut = XML.toJSONObject(writer.getBuffer().toString());

        FileWriter fileWriter = new FileWriter("JSON/" + rcnValue + ".json");
        fileWriter.write(jsonOut.toString(4));
        fileWriter.close();

        PutDocuments("JSON/" + rcnValue + ".json");
    }
}
