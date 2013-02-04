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

import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.search.Field;
import com.liferay.portal.kernel.search.QuerySuggester;
import com.liferay.portal.kernel.search.SearchContext;
import com.liferay.portal.kernel.search.SearchException;
import com.liferay.portal.kernel.util.StringBundler;
import com.liferay.portal.kernel.util.StringPool;
import com.liferay.portal.search.solr.analyzer.DefaultAnalizer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.search.spell.StringDistance;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

/**
 * @author Michael C. Han
 */
public class SolrQuerySuggesterImpl implements QuerySuggester {

	public void searchTokenSimilars(
			Locale locale, String original, SolrQuery solrQuery, String token,
			Map<String, Float> words)
		throws SearchException {

		solrQuery.addFilterQuery("spellcheck:true");
		solrQuery.addFilterQuery("locale:" + locale.toString());

		try {
			QueryResponse queryResponse = _solrServer.query(
				solrQuery, SolrRequest.METHOD.POST);

			SolrDocumentList solrDocumentList = queryResponse.getResults();

			int numResults = solrDocumentList.size();

			Map<String, Float> tokenSuggestions = new HashMap<String, Float>();

			boolean foundWord = false;

			for (int i = 0; i < numResults; i++) {

				SolrDocument solrDocument = solrDocumentList.get(i);

				String suggestion =
					((List<String>)solrDocument.get("word")).get(0);

				String strWeight =
					((List<String>)solrDocument.get("weight")).get(0);
				float weight = Float.parseFloat(strWeight);

				if (suggestion.equalsIgnoreCase(token)) {
					words.put(original, weight);
					foundWord = true;
					break;
				}

				float distance = _stringDistance.getDistance(token, suggestion);

				if (distance > _threshold) {
					Float normalizedWeight = weight + distance;
					tokenSuggestions.put(suggestion, normalizedWeight);
				}
			}

			if (!foundWord) {
				if (tokenSuggestions.isEmpty()) {
					words.put(original, 0f);
				}
				else {
					words.putAll(tokenSuggestions);
				}
			}
		}
		catch (Exception e) {
			if (_log.isDebugEnabled()) {
				_log.debug("Unable to execute Solr query", e);
			}

			throw new SearchException(e.getMessage());
		}

	}

	public void setSolrServer(SolrServer solrServer) {
		_solrServer = solrServer;
	}

	public void setStringDistance(StringDistance stringDistance) {
		_stringDistance = stringDistance;
	}

	public String spellCheckKeywords(SearchContext searchContext)
		throws SearchException {

		String collated = StringPool.BLANK;

		Map<String, List<String>> map = spellCheckKeywords(searchContext, 1);

		String keywords = searchContext.getKeywords();
		List<String> tokens = DefaultAnalizer.tokenize(keywords);

		for (String token : tokens) {
			if (!map.get(token).isEmpty()) {

				String suggestion = map.get(token).get(0);

				if (Character.isUpperCase(token.charAt(0))) {
					suggestion = suggestion.substring(0, 1).toUpperCase()
						.concat(suggestion.substring(1));
				}

				collated = collated.concat(suggestion)
					.concat(StringPool.SPACE);
			}
			else {
				collated = collated.concat(token).concat(StringPool.SPACE);
			}
		}

		if (!collated.equals(StringPool.BLANK)) {
			collated = collated.substring(0, collated.length()-1);
		}

		return collated;
	}

	public Map<String, List<String>> spellCheckKeywords(
			SearchContext searchContext, int maxSuggestions)
		throws SearchException {

		Map<String, List<String>> suggestions =
			new HashMap<String, List<String>>();

		Locale locale = searchContext.getLocale();

		List<String> originals = DefaultAnalizer.tokenize(
			searchContext.getKeywords());

		for (String original : originals) {
			String token = original;

			List<String> similarTokens = suggestTokenSimilars(
				locale, maxSuggestions, original, token);

			suggestions.put(original, similarTokens);
		}

		return suggestions;
	}

	public String[] suggestKeywordQueries(SearchContext searchContext, int max)
		throws SearchException {

		SolrQuery solrQuery = new SolrQuery();

		StringBundler sb = new StringBundler(5);

		sb.append(Field.KEYWORD_SEARCH);
		sb.append(StringPool.COLON);
		sb.append(StringPool.QUOTE);
		sb.append(searchContext.getKeywords());
		sb.append(StringPool.QUOTE);

		solrQuery.setRequestHandler(_suggesterURL);
		solrQuery.setQuery(sb.toString());
		solrQuery.setRows(max);

		String companyIdFilterQuery = Field.COMPANY_ID.concat(
			StringPool.COLON).concat(
			Long.toString(searchContext.getCompanyId()));

		solrQuery.setFilterQueries(companyIdFilterQuery);

		try {
			QueryResponse queryResponse = _solrServer.query(solrQuery);

			SolrDocumentList solrDocumentList = queryResponse.getResults();

			int numDocuments = solrDocumentList.size();

			String[] results = new String[numDocuments];

			for (int i = 0; i < numDocuments; i++) {
				SolrDocument solrDocument = solrDocumentList.get(i);

				results[i] = (String)solrDocument.getFieldValue(
					Field.KEYWORD_SEARCH);
			}

			return results;
		}
		catch (Exception e) {
			if (_log.isDebugEnabled()) {
				_log.debug("Unable to execute Solr query", e);
			}

			throw new SearchException(e.getMessage());
		}
	}

	public List<String> suggestTokenSimilars(
			Locale locale, int maxSuggestions, String original, String token)
		throws SearchException {

		DefaultAnalizer result = DefaultAnalizer.analyze(token);

		SolrQuery solrQuery = new SolrQuery();

		StringBundler sb =new StringBundler(10);

		sb.append(addGramsQuery("gram2", result.gram2s));
		sb.append(addGramsQuery("gram3", result.gram3s));
		sb.append(addGramsQuery("gram4", result.gram4s));

		sb.append(addGramQuery("start2", result.start2));
		sb.append(addGramQuery("start3", result.start3));
		sb.append(addGramQuery("start4", result.start4));

		sb.append(addGramQuery("end2", result.end2));
		sb.append(addGramQuery("end3", result.end3));
		sb.append(addGramQuery("end4", result.end4));

		String wordQuery = "word".concat(StringPool.COLON).concat(result.input);

		sb.append(wordQuery);

		solrQuery.setQuery(sb.toString());

		Map<String, Float> words = new HashMap<String, Float>();

		searchTokenSimilars(locale, original, solrQuery, token, words);

		ValueComparator bvc = new ValueComparator(words);
		TreeMap<String, Float> sortedWords = new TreeMap<String, Float>(bvc);

		sortedWords.putAll(words);

		List<String> listWords = new ArrayList(sortedWords.keySet());

		return listWords.subList(0, Math.min(maxSuggestions, listWords.size()));

	}

	private String addGramQuery(String fieldName, String fieldValue) {

		StringBundler sb =new StringBundler(6);

		sb.append(fieldName);
		sb.append(StringPool.COLON);
		sb.append(fieldValue);
		sb.append(StringPool.SPACE);
		sb.append("OR");
		sb.append(StringPool.SPACE);

		return sb.toString();
	}

	private String addGramsQuery(String fieldName, List<String> grams) {

		StringBundler sb = new StringBundler(grams.size());

		for (String gram : grams) {
			sb.append(addGramQuery(fieldName, gram));
		}

		return sb.toString();
	}

	private static Log _log = LogFactoryUtil.getLog(
		SolrQuerySuggesterImpl.class);

	private SolrServer _solrServer;

	private StringDistance _stringDistance;

	private String _suggesterURL = "/select";

	private float _threshold = 0.8f;

	private class ValueComparator implements Comparator<String> {

		Map<String, Float> base;
		public ValueComparator(Map<String, Float> base) {
			this.base = base;
		}

		public int compare(String a, String b) {
			if (base.get(a) >= base.get(b)) {
				return -1;
			}
			else {
				return 1;
			}
		}

	}

}