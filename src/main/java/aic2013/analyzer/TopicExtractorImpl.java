package aic2013.analyzer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import aic2013.analyzer.filter.BaseFilter;
import aic2013.analyzer.filter.GlobalPrefixFilter;
import aic2013.analyzer.filter.PrefixFilter;
import aic2013.analyzer.filter.TextFilter;
import aic2013.common.entities.Topic;
import cc.mallet.classify.tui.Text2Vectors;
import cc.mallet.pipe.CharSequence2TokenSequence;
import cc.mallet.pipe.CharSequenceRemoveHTML;
import cc.mallet.pipe.CharSubsequence;
import cc.mallet.pipe.FeatureSequence2AugmentableFeatureVector;
import cc.mallet.pipe.Input2CharSequence;
import cc.mallet.pipe.Pipe;
import cc.mallet.pipe.PrintInputAndTarget;
import cc.mallet.pipe.SaveDataInSource;
import cc.mallet.pipe.SerialPipes;
import cc.mallet.pipe.Target2Label;
import cc.mallet.pipe.TokenSequence2FeatureSequence;
import cc.mallet.pipe.TokenSequence2FeatureSequenceWithBigrams;
import cc.mallet.pipe.TokenSequenceLowercase;
import cc.mallet.pipe.TokenSequenceNGrams;
import cc.mallet.pipe.TokenSequenceRemoveNonAlpha;
import cc.mallet.pipe.TokenSequenceRemoveStopwords;
import cc.mallet.pipe.iterator.FileIterator;
import cc.mallet.topics.ParallelTopicModel;
import cc.mallet.topics.tui.Vectors2Topics;
import cc.mallet.types.FeatureSequence;
import cc.mallet.types.InstanceList;
import cc.mallet.util.CharSequenceLexer;
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

	private final Pipe instancePipe;

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

		instancePipe = createMalletPipe(false, false, true, true);
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

//			Text2Vectors t2v = new Text2Vectors();

			// form string array args and invoke main of Text2Vectors.
//			String[] argsT2v = { "--remove-stopwords", "true",
//					"--preserve-case", "false", "--input",
//					rawTweetDir.toString(), "--output", vectorsFile.getPath(),
//					"--keep-sequence" };
//			Text2Vectors.main(argsT2v);
			InstanceList instances = text2Vectors(rawTweetDir.toString());

//			Vectors2Topics v2t = new Vectors2Topics();
			
			

			// form string array args and invoke main of Vectors2Topics.
//			String[] argsV2t = { "--num-iterations", "200",
//					"--optimize-interval", "10", "--num-top-words", "3",
//					"--doc-topics-threshold", "0.26", "--input",
//					vectorsFile.getPath(), "--num-topics", "1",
//					// "--output-state", <output directory
//					// path>+"/output_state.gz",
//					"--output-topic-keys", topicsFile.getPath()
//			// path>+"/output_topic_keys",
//			// "--output-doc-topics", topicsFile.getPath()
//			};
//			
//			try {
//				Vectors2Topics.main(argsV2t);
//			} catch (IllegalArgumentException e) {
//				System.err.println("Tweet: " + input);
//				System.err.println("rawTweetFile = " + rawTweetFile.getPath());
//				System.err.println("vectorsFile = " + vectorsFile.getPath());
//				System.err.println("topicsFile = " + topicsFile.getPath());
//				e.printStackTrace();
//			}
			
			Set<Topic> extractedTopics = null;
			try {
				extractedTopics = vectors2Topic(instances, 3, 200, 10, 1);
			} catch (IllegalArgumentException e) {
				System.err.println("Tweet: " + input);
				System.err.println("rawTweetFile = " + rawTweetFile.getPath());
				System.err.println("vectorsFile = " + vectorsFile.getPath());
				System.err.println("topicsFile = " + topicsFile.getPath());
				e.printStackTrace();
			} catch(IOException e){
				e.printStackTrace();
			}

			// Stanford NER

			List<List<CoreLabel>> out = classifier.classify(filteredInput);
			for (List<CoreLabel> sentence : out) {
				String prevAnnotation = null;
				Topic prevTopic = null;
				for (CoreLabel word : sentence) {
					String annotation = word
							.get(CoreAnnotations.AnswerAnnotation.class);
					if (annotation.equals(prevAnnotation)) {
						// put connected words that have the same annotation
						// into the same topic
						// like "Joe/PERSON Miller/PERSON" --> Topic(["Joe",
						// "Miller"])
						prevTopic.getName()[0] += " " + word.word();
					} else if (!"O".equals(annotation)) {
						// some different annotation than the preceding one
						prevAnnotation = annotation;
						prevTopic = new Topic(new String[] { word.word() });
						extractedTopics.add(prevTopic);
					} else {
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

	private Pipe createMalletPipe(boolean keepSequenceBigrams,
			boolean preserveCase, boolean removeStopwords, boolean keepSequence) {
		// Create a list of pipes that will be added to a SerialPipes object
		// later
		ArrayList<Pipe> pipeList = new ArrayList<Pipe>();

		// Convert the "target" object into a numeric index
		// into a LabelAlphabet.
		pipeList.add(new Target2Label());

		// The "data" field is currently a filename. Save it as "source".
		pipeList.add(new SaveDataInSource());

		// Set "data" to the file's contents. "data" is now a String.
		pipeList.add(new Input2CharSequence(Charset.defaultCharset()
				.displayName()));

		// Optionally save the text to "source" -- not recommended if memory is
		// scarce.
		// if (saveTextInSource.wasInvoked()) {
		// pipeList.add(new SaveDataInSource());
		// }

		// Allow the user to specify an arbitrary Pipe object
		// that operates on Strings
		// if (stringPipe.wasInvoked()) {
		// pipeList.add((Pipe) stringPipe.value);
		// }

		// Remove all content before the first empty line.
		// Useful for email and usenet news posts.
		// if (skipHeader.value) {
		// pipeList.add(new CharSubsequence(CharSubsequence.SKIP_HEADER));
		// }

		// Remove HTML tags. Suitable for SGML and XML.
		// if (skipHtml.value) {
		// pipeList.add(new CharSequenceRemoveHTML());
		// }

		//
		// Tokenize the input: first compile the tokenization pattern
		//

		Pattern tokenPattern = null;

		if (keepSequenceBigrams) {
			// We do not want to record bigrams across punctuation,
			// so we need to keep non-word tokens.
			tokenPattern = CharSequenceLexer.LEX_NONWHITESPACE_CLASSES;
		} else {
			// Otherwise, try to compile the regular expression pattern.
			String tokenRegex = CharSequenceLexer.LEX_ALPHA.toString();
			try {
				tokenPattern = Pattern.compile(tokenRegex);
			} catch (PatternSyntaxException pse) {
				throw new IllegalArgumentException(
						"The token regular expression (" + tokenRegex
								+ ") was invalid: " + pse.getMessage());
			}
		}

		// Add the tokenizer
		pipeList.add(new CharSequence2TokenSequence(tokenPattern));

		// Allow user to specify an arbitrary Pipe object
		// that operates on TokenSequence objects.
		// if (tokenPipe.wasInvoked()) {
		// pipeList.add((Pipe) tokenPipe.value);
		// }

		if (!preserveCase) {
			pipeList.add(new TokenSequenceLowercase());
		}

		if (keepSequenceBigrams) {
			// Remove non-word tokens, but record the fact that they
			// were there.
			pipeList.add(new TokenSequenceRemoveNonAlpha(true));
		}

		// Stopword removal.
		// if (stoplistFile.wasInvoked()) {
		//
		// // The user specified a new list
		//
		// TokenSequenceRemoveStopwords stopwordFilter = new
		// TokenSequenceRemoveStopwords(
		// stoplistFile.value, encoding.value, false, // don't include
		// // default list
		// false, keepSequenceBigrams.value);
		//
		// if (extraStopwordsFile.wasInvoked()) {
		// stopwordFilter.addStopWords(extraStopwordsFile.value);
		// }
		//
		// pipeList.add(stopwordFilter);
		// } else
		if (removeStopwords) {

			// The user did not specify a new list, so use the default
			// built-in English list, possibly adding extra words.

			TokenSequenceRemoveStopwords stopwordFilter = new TokenSequenceRemoveStopwords(
					false, keepSequenceBigrams);

			// if (extraStopwordsFile.wasInvoked()) {
			// stopwordFilter.addStopWords(extraStopwordsFile.value);
			// }

			pipeList.add(stopwordFilter);

		}

		// gramSizes is an integer array, with default value [1].
		// Check if we have a non-default value.
		int[] gramSizes = new int[] { 1 };
		if (!(gramSizes.length == 1 && gramSizes[0] == 1)) {
			pipeList.add(new TokenSequenceNGrams(gramSizes));
		}

		// So far we have a sequence of Token objects that contain
		// String values. Look these up in an alphabet and store integer IDs
		// ("features") instead of Strings.
		if (keepSequenceBigrams) {
			pipeList.add(new TokenSequence2FeatureSequenceWithBigrams());
		} else {
			pipeList.add(new TokenSequence2FeatureSequence());
		}

		// For many applications, we do not need to preserve the sequence of
		// features,
		// only the number of times times a feature occurs.
		if (!(keepSequence || keepSequenceBigrams)) {
			boolean binaryFeatures = false;
			pipeList.add(new FeatureSequence2AugmentableFeatureVector(
					binaryFeatures));
		}

		// Allow users to specify an arbitrary Pipe object that operates on
		// feature vectors.
		// if (featureVectorPipe.wasInvoked()) {
		// pipeList.add((Pipe) featureVectorPipe.value);
		// }

		// if (printOutput.value) {
		// pipeList.add(new PrintInputAndTarget());
		// }

		return new SerialPipes(pipeList);
	}

	private InstanceList text2Vectors(String rawTweetDir) {
		InstanceList instances = new InstanceList(instancePipe);

		boolean removeCommonPrefix = true;
		instances.addThruPipe(new FileIterator(new String[] { rawTweetDir },
				FileIterator.STARTING_DIRECTORIES, removeCommonPrefix));

		return instances;
		// write vector file
		// ObjectOutputStream oos;
		// if (outputFile.value.toString().equals ("-")) {
		// oos = new ObjectOutputStream(System.out);
		// }
		// else {
		// oos = new ObjectOutputStream(new FileOutputStream(outputFile.value));
		// }
		// oos.writeObject(instances);
		// oos.close();
	}

	private Set<Topic> vectors2Topic(InstanceList training, int numTopics, int numIterations, int optimizeInterval, int numThreads) throws IOException {
		// Start a new LDA topic model

		ParallelTopicModel topicModel = null;

		System.out.println("Data loaded.");

		if (training.size() > 0 && training.get(0) != null) {
			Object data = training.get(0).getData();
			if (!(data instanceof FeatureSequence)) {
				System.err
						.println("Topic modeling currently only supports feature sequences: use --keep-sequence option when importing data.");
				System.exit(1);
			}
		}

		double alpha = 50.0;
		double beta = 0.01;
		topicModel = new ParallelTopicModel(numTopics, alpha,
				beta);
//		if (randomSeed.value != 0) {
//			topicModel.setRandomSeed(randomSeed.value);
//		}

		topicModel.addInstances(training);

		int showTopicsInterval = 50;
		int topWords = 20;
		topicModel.setTopicDisplay(showTopicsInterval, topWords);

		/*
		 * if (testingFile.value != null) { topicModel.setTestingInstances(
		 * InstanceList.load(new File(testingFile.value)) ); }
		 */

		topicModel.setNumIterations(numIterations);
		topicModel.setOptimizeInterval(optimizeInterval);
		int optimizeBurnIn = 200;
		topicModel.setBurninPeriod(optimizeBurnIn);
		boolean useSymmetricAlpha = false;
		topicModel.setSymmetricAlpha(useSymmetricAlpha);

//		int outputStateInterval = 0;
//		if (outputStateInterval != 0) {
//			topicModel.setSaveState(outputStateInterval, stateFile.value);
//		}

//		if (outputModelInterval.value != 0) {
//			topicModel.setSaveSerializedModel(outputModelInterval.value,
//					outputModelFilename.value);
//		}

		topicModel.setNumThreads(numThreads);

		topicModel.estimate();

//		if (topicKeysFile.value != null) {
//			topicModel.printTopWords(new File(topicKeysFile.value),
//					topWords.value, false);
//		}
		
		Object[][] topicStrings = topicModel.getTopWords(topWords);
		Set<Topic> result = new HashSet<>();
		for(int i = 0; i < topicStrings.length; i++){
			String[] keywords = new String[topicStrings[i].length];
			for(int j = 0; j < keywords.length; j++){
				keywords[j] = (String) topicStrings[i][j];
			}
			result.add(new Topic(keywords));
		}
		return result;

//		if (topicReportXMLFile.value != null) {
//			PrintWriter out = new PrintWriter(topicReportXMLFile.value);
//			topicModel.topicXMLReport(out, topWords.value);
//			out.close();
//		}

//		if (topicPhraseReportXMLFile.value != null) {
//			PrintWriter out = new PrintWriter(topicPhraseReportXMLFile.value);
//			topicModel.topicPhraseXMLReport(out, topWords.value);
//			out.close();
//		}

//		if (stateFile.value != null) {
//			topicModel.printState(new File(stateFile.value));
//		}

//		if (docTopicsFile.value != null) {
//			PrintWriter out = new PrintWriter(new FileWriter((new File(
//					docTopicsFile.value))));
//			topicModel.printDocumentTopics(out, docTopicsThreshold.value,
//					docTopicsMax.value);
//			out.close();
//		}
//
//		if (topicWordWeightsFile.value != null) {
//			topicModel.printTopicWordWeights(new File(
//					topicWordWeightsFile.value));
//		}
//
//		if (wordTopicCountsFile.value != null) {
//			topicModel
//					.printTypeTopicCounts(new File(wordTopicCountsFile.value));
//		}
//
//		if (outputModelFilename.value != null) {
//			assert (topicModel != null);
//			try {
//
//				ObjectOutputStream oos = new ObjectOutputStream(
//						new FileOutputStream(outputModelFilename.value));
//				oos.writeObject(topicModel);
//				oos.close();
//
//			} catch (Exception e) {
//				e.printStackTrace();
//				throw new IllegalArgumentException(
//						"Couldn't write topic model to filename "
//								+ outputModelFilename.value);
//			}
//		}
//
//		if (inferencerFilename.value != null) {
//			try {
//
//				ObjectOutputStream oos = new ObjectOutputStream(
//						new FileOutputStream(inferencerFilename.value));
//				oos.writeObject(topicModel.getInferencer());
//				oos.close();
//
//			} catch (Exception e) {
//				System.err.println(e.getMessage());
//			}
//
//		}
//
//		if (evaluatorFilename.value != null) {
//			try {
//
//				ObjectOutputStream oos = new ObjectOutputStream(
//						new FileOutputStream(evaluatorFilename.value));
//				oos.writeObject(topicModel.getProbEstimator());
//				oos.close();
//
//			} catch (Exception e) {
//				System.err.println(e.getMessage());
//			}
//
//		}
	}
	
}
