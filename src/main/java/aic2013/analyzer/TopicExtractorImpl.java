package aic2013.analyzer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import aic2013.analyzer.filter.BaseFilter;
import aic2013.analyzer.filter.GlobalPrefixFilter;
import aic2013.analyzer.filter.PrefixFilter;
import aic2013.analyzer.filter.TextFilter;
import aic2013.common.entities.Topic;
import cc.mallet.classify.tui.Text2Vectors;
import cc.mallet.topics.tui.Vectors2Topics;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;

/**
 * @author Moritz Becker (moritz.becker@gmx.at)
 * 
 */
public class TopicExtractorImpl implements TopicExtractor {
	private static Logger logger = Logger.getLogger(TopicExtractorImpl.class
			.getName());

	private final File rawTweetFile;
	private final Path rawTweetDir;
	private final File vectorsFile;
	private final File topicsFile;
	private final TextFilter filter;
	private final AbstractSequenceClassifier<CoreLabel> classifier;

	private final CharsetEncoder asciiEncoder = Charset.forName("US-ASCII")
			.newEncoder();

	public TopicExtractorImpl() throws IOException {
		rawTweetDir = Files.createTempDirectory("rawTweetDir");
		rawTweetFile = Files.createTempFile(rawTweetDir, "rawTweet", ".txt")
				.toFile();
		rawTweetFile.deleteOnExit();
		vectorsFile = Files.createTempFile("tweetVectors", ".mallet").toFile();
		vectorsFile.deleteOnExit();
		topicsFile = Files.createTempFile("tweetTopics", ".txt").toFile();
		topicsFile.deleteOnExit();
		filter = new PrefixFilter("#", new PrefixFilter("@", new PrefixFilter(
				"http", new GlobalPrefixFilter("RT", false, new BaseFilter()))));

		classifier = CRFClassifier
				.getClassifierNoExceptions("classifiers/english.all.3class.distsim.crf.ser.gz");
	}

	@Override
	public Set<Topic> extract(String input) throws ExtractionException {
		PrintStream rawTweetOutStream = null;
		BufferedReader topicInputStream = null;
		try {
			if (!asciiEncoder.canEncode(input)) {
				logger.log(Level.WARNING, "Non-ASCII tweet encountered");
				return new HashSet<Topic>();
			}

			String filteredInput = filter.filter(input);
			/* write input to temporary file */
			rawTweetOutStream = new PrintStream(new FileOutputStream(
					rawTweetFile));
			rawTweetOutStream.print(filteredInput);
			rawTweetOutStream.close();
			rawTweetOutStream = null;

			Text2Vectors t2v = new Text2Vectors();

			// form string array args and invoke main of Text2Vectors.
			String[] argsT2v = { "--remove-stopwords", "true",
					"--preserve-case", "false", "--input",
					rawTweetDir.toString(), "--output", vectorsFile.getPath(),
					"--keep-sequence" };
			Text2Vectors.main(argsT2v);

			Vectors2Topics v2t = new Vectors2Topics();

			// form string array args and invoke main of Vectors2Topics.
			String[] argsV2t = { "--num-iterations", "200",
					"--optimize-interval", "10", "--num-top-words", "3",
					"--doc-topics-threshold", "0.26", "--input",
					vectorsFile.getPath(), "--num-topics", "1",
					// "--output-state", <output directory
					// path>+"/output_state.gz",
					"--output-topic-keys", topicsFile.getPath()
			// path>+"/output_topic_keys",
			// "--output-doc-topics", topicsFile.getPath()
			};

			try {
				Vectors2Topics.main(argsV2t);
			} catch (IllegalArgumentException e) {
				System.err.println("Tweet: " + input);
				System.err.println("rawTweetFile = " + rawTweetFile.getPath());
				System.err.println("vectorsFile = " + vectorsFile.getPath());
				System.err.println("topicsFile = " + topicsFile.getPath());
				e.printStackTrace();
			}

			Set<Topic> extractedTopics = readTopicsFromFile(topicsFile);

			// Stanford NER

			List<List<CoreLabel>> out = classifier.classify(filteredInput);
			for (List<CoreLabel> sentence : out) {
				String prevAnnotation = null;
				Topic prevTopic = null;
				for (CoreLabel word : sentence) {
					String annotation = word.get(CoreAnnotations.AnswerAnnotation.class);
					if(annotation.equals(prevAnnotation)){
						// put connected words that have the same annotation into the same topic
						// like "Joe/PERSON Miller/PERSON" --> Topic(["Joe", "Miller"])
						prevTopic.getName()[0] += " " + word.word();
					}else if(! "O".equals(annotation)){
						// some different annotation than the preceding one
						prevAnnotation = annotation;
						prevTopic = new Topic(new String[]{word.word()});
						extractedTopics.add(prevTopic);
					}else{
						// we have a /O annotation
						prevAnnotation = null;
					}
					System.out.print(word.word() + '/'
							+ word.get(CoreAnnotations.AnswerAnnotation.class)
							+ ' ');
				}
				System.out.println();
			}
			// out = classifier.classifyFile(args[1]);
			// for (List<CoreLabel> sentence : out) {
			// for (CoreLabel word : sentence) {
			// System.out.print(word.word() + '/' +
			// word.get(CoreAnnotations.AnswerAnnotation.class) + ' ');
			// }
			// System.out.println();
			// }
			return extractedTopics;

		} catch (IOException e) {
			throw new ExtractionException(e);
		} finally {
			try {
				if (rawTweetOutStream != null) {
					rawTweetOutStream.close();
				}
				if (topicInputStream != null) {
					topicInputStream.close();
				}
			} catch (IOException e) {
				throw new ExtractionException(e);
			}
		}
	}

	private Set<Topic> readTopicsFromFile(File topicsFile) throws IOException {
		BufferedReader topicInputStream = null;

		try {
			topicInputStream = new BufferedReader(new InputStreamReader(
					new FileInputStream(topicsFile)));
			String line;

			Set<Topic> extractedTopics = new HashSet<>();
			while ((line = topicInputStream.readLine()) != null) {
				System.out.println(line);
				String[] parts = line.split("\t");
				if (parts.length != 3)
					break;
				extractedTopics.add(new Topic(parts[2].split(" ")));

			}
			return extractedTopics;
		} finally {
			if (topicInputStream != null)
				topicInputStream.close();
		}
	}

}
