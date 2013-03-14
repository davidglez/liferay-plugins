/**
 * Copyright (c) 2000-2012 Liferay, Inc. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 */

package com.liferay.portal.search.solr;

import com.liferay.portal.kernel.search.SearchException;

import java.io.IOException;
import java.io.StringReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

/**
 * @author Daniela Zapata
 * @author David Gonzalez
 */
public class SolrSpellCheckBaseImpl {

	public void setAnalyzer(Analyzer analyzer) {
		_analyzer = analyzer;
	}

	protected Map<String, Object> buildNGrams(String input, int ng1, int ng2)
		throws SearchException {

		Map<String, Object> ngramsMap =
			new HashMap<String, Object>();

		NGramTokenizer nGramTokenizer =
			new NGramTokenizer(new StringReader(input), ng1, ng2);

		CharTermAttribute charTermAttribute = nGramTokenizer.getAttribute(
			CharTermAttribute.class);
		OffsetAttribute offsetAttribute = nGramTokenizer.getAttribute(
			OffsetAttribute.class);

		List<String> gram2 = new ArrayList<String>();
		List<String> gram3 = new ArrayList<String>();
		List<String> gram4 = new ArrayList<String>();

		try {

			int length = input.length();

			while (nGramTokenizer.incrementToken()) {
				String nGram = charTermAttribute.toString();
				int nGramSize = charTermAttribute.length();

				if (nGramSize == 2) {
					if(offsetAttribute.startOffset() == 0){
						ngramsMap.put("start2", nGram);
					}
					else if(offsetAttribute.endOffset() == length){
						ngramsMap.put("end2", nGram);
					}
					else {
						gram2.add(nGram);
					}
				}
				else if (nGramSize == 3) {
					if(offsetAttribute.startOffset() == 0){
						ngramsMap.put("start3", nGram);
					}
					if(offsetAttribute.endOffset() == length){
						ngramsMap.put("end3", nGram);
					}
					else {
						gram3.add(nGram);
					}
				}
				else if (nGramSize == 4) {
					if(offsetAttribute.startOffset() == 0){
						ngramsMap.put("start4", nGram);
					}
					if(offsetAttribute.endOffset() == length){
						ngramsMap.put("end4", nGram);
					}
					else {
						gram4.add(nGram);
					}
				}
				else {
					continue;
				}
			}

			if (!gram2.isEmpty())
				ngramsMap.put("gram2", gram2);

			if (!gram3.isEmpty())
				ngramsMap.put("gram3", gram3);

			if (!gram4.isEmpty())
				ngramsMap.put("gram4", gram4);

			return ngramsMap;

		}
		catch (IOException e) {
			e.printStackTrace();

			throw new SearchException();
		}

	}

	protected int getMax(int l) {
		if (l > 5) {
			return 4;
		}
		else {
			return 3;
		}
	}

	protected int getMin(int l) {
		if (l > 5) {
			return 3;
		}
		else {
			return 2;
		}
	}

	protected Analyzer _analyzer;
}
