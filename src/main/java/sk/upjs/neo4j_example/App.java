package sk.upjs.neo4j_example;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.FileUtils;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

public class App {
	public static void main(String[] args) throws IOException {
		Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		logger.setLevel(Level.INFO);
//		FileUtils.deleteRecursively(new File(Neo4jConfig.LOCAL_STORE_DIR));
		
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Neo4jConfig.class);
		DBService dbService = context.getBean(DBService.class);
//		dbService.openAndFillDb();
		List<Node> seedNodes = dbService.getSeedNodes();
		dbService.printNodes(seedNodes);
//		dbService.printDownloadTree(seedNodes.get(0));
		dbService.printShortestPathsToDetailPages(seedNodes.get(0));
		dbService.shutDown();
	}
}
