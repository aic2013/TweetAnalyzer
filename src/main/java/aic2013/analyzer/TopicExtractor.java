package aic2013.analyzer;

import java.util.Set;

import aic2013.common.entities.Topic;


/**
 * @author Moritz Becker (moritz.becker@gmx.at)
 *
 */
public interface TopicExtractor {
	Set<Topic> extract(String input) throws ExtractionException;
}
