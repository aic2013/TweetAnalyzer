/**
 * 
 */
package aic2013.analyzer.filter;

/**
 * @author Moritz Becker (moritz.becker@gmx.at)
 *
 */
public class PrefixFilter extends TextFilterDecorator {

	private String prefix;
	/**
	 * @param decoratedFilter
	 */
	public PrefixFilter(String prefix, TextFilter decoratedFilter) {
		super(decoratedFilter);
		this.prefix = prefix;
	}
	
	/* (non-Javadoc)
	 * @see aic2013.extractor.TextFilterDecorator#filter(java.lang.String)
	 */
	@Override
	public String filter(String input) {
		StringBuilder sb = new StringBuilder();
		for(String word : input.split(" ")){
			if(!word.startsWith(prefix)){
				sb.append(word).append(" ");
			}
		}
		if(sb.length() > 0){
			sb.deleteCharAt(sb.length() - 1);
		}
		return decoratedFilter.filter(sb.toString());
	}

}
