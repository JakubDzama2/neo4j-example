package sk.upjs.neo4j_example;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "sk.upjs.neo4j_example")
public class Neo4jConfig {

	public static final String LOCAL_STORE_DIR = "lokalnedb/1/embeded";
	
	@Bean
	public GraphDatabaseService getGraphDb() {
		GraphDatabaseFactory graphDatabaseFactory = new GraphDatabaseFactory();
		return graphDatabaseFactory.newEmbeddedDatabase(new File(LOCAL_STORE_DIR));
	}
}
