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

package com.liferay.portal.search.solr.analyzer;

import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.search.SearchException;

import java.io.IOException;
import java.io.StringReader;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

/**
 * @author Daniela Zapata
 * @author David Mendez
 */
public class DefaultAnalizer {

	public static DefaultAnalizer analyze(String input) throws SearchException {

		DefaultAnalizer result = new DefaultAnalizer();
		result.input = input;
		TokenStream tokenStream =
			new NGramTokenizer(new StringReader(input), 2, 4);

		CharTermAttribute charTermAttribute = tokenStream.getAttribute(
			CharTermAttribute.class);

		try {
			while (tokenStream.incrementToken()) {
				String text = charTermAttribute.toString();

				if (text.length() == 2) {
					result.gram2s.add(text);
				}
				else if (text.length() == 3) {
					result.gram3s.add(text);
				}
				else if (text.length() == 4) {
					result.gram4s.add(text);
				}
				else {
					continue;
				}
			}
		}
		catch (IOException e) {
			e.printStackTrace();

			throw new SearchException();
		}

		result.start2 = input.substring(0, Math.min(input.length(), 2));
		result.start3 = input.substring(0, Math.min(input.length(), 3));
		result.start4 = input.substring(0, Math.min(input.length(), 4));

		result.end2 = input.substring(
			Math.max(0, input.length() - 2), input.length());
		result.end3 = input.substring(
			Math.max(0, input.length() - 3), input.length());
		result.end4 = input.substring(
			Math.max(0, input.length() - 4), input.length());

		return result;
	}

	public static List<String> tokenize(String keyword) throws SearchException {

		List<String> result = new ArrayList<String>();

		Analyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_40);

		try {
			TokenStream tokenStream = analyzer.tokenStream(
				null, new StringReader(keyword));

			CharTermAttribute charTermAttribute = tokenStream.addAttribute(
				CharTermAttribute.class);

			tokenStream.reset();

			while (tokenStream.incrementToken()) {
				String temp = charTermAttribute.toString();
				String token = temp.replaceAll("[^A-Za-z0-9]", "");
				result.add(token);
			}

			tokenStream.end();
			tokenStream.close();
		}
		catch (IOException e) {
			if (_log.isDebugEnabled()) {
				_log.debug("Unable to tokenize query", e);

				throw new SearchException();
			}
		}

		return result;
	}

	public String end2 = "";

	public String end3 = "";

	public String end4 = "";

	public List<String> gram2s = new ArrayList<String>();
	public List<String> gram3s = new ArrayList<String>();
	public List<String> gram4s = new ArrayList<String>();
	public String input;
	public String start2 = "";
	public String start3 = "";
	public String start4 = "";

	private static Log _log = LogFactoryUtil.getLog(DefaultAnalizer.class);

}