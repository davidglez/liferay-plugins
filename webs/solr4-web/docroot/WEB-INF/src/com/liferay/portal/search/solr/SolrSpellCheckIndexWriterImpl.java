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
import com.liferay.portal.kernel.search.SearchContext;
import com.liferay.portal.kernel.search.SearchException;
import com.liferay.portal.kernel.search.SpellCheckIndexWriter;
import com.liferay.portal.kernel.util.LocaleUtil;
import com.liferay.portal.kernel.util.StringBundler;
import com.liferay.portal.kernel.util.StringPool;
import com.liferay.util.portlet.PortletProps;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;

/**
 * @author Michael C. Han
 */
public class SolrSpellCheckIndexWriterImpl extends SolrSpellCheckBaseImpl
	implements SpellCheckIndexWriter {

	public void deleteDocuments() throws SearchException {
		try {

			_solrServer.deleteByQuery("spellcheck:true");

			if (_commit) {
				_solrServer.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void indexDictionaries(SearchContext searchContext)
		throws SearchException {

		deleteDocuments();

		for (String supportedLocale : _supportedLocales) {
			searchContext.setLocale(LocaleUtil.fromLanguageId(supportedLocale));

			indexDictionary(searchContext);
		}
	}

	public void indexDictionary(SearchContext searchContext)
		throws SearchException {

		Locale locale = searchContext.getLocale();
		String strLocale = locale.toString();

		String dictionaryDir = PortletProps.get(DICTIONARY_DIRECTORY);
		String dictionaryRelativePath = dictionaryDir.concat(
			strLocale).concat(DICTIONARY_EXTENSION_FILE);

		String dictionaryCompletePath =
			SolrSpellCheckIndexWriterImpl.class.getClassLoader()
				.getResource(dictionaryRelativePath).getFile();
        File fileExternal = new File(dictionaryCompletePath);
        doIndexDictionary(fileExternal, locale);

		StringBundler customRelativePath = new StringBundler(5);
		String customDir = PortletProps.get(DICTIONARY_DIRECTORY);
		customRelativePath.append(customDir);
		customRelativePath.append(CUSTOM_PREFIX);
		customRelativePath.append(StringPool.UNDERLINE);
		customRelativePath.append(strLocale);
		customRelativePath.append(CUSTOM_EXTENSION_FILE);

		String customCompletePath =
			SolrSpellCheckIndexWriterImpl.class.getClassLoader()
				.getResource(customRelativePath.toString()).getFile();
        File fileCustom = new File(customCompletePath);
        doIndexDictionary(fileCustom, locale);
	}

	public void setCommit(boolean commit) {
		_commit = commit;
	}

	public void setSolrServer(SolrServer solrServer) {
		_solrServer = solrServer;
	}

	public void setSupportedLocales(Set<String> supportedLocales) {
		_supportedLocales = supportedLocales;
	}

	private void addDocument(Set<SolrInputDocument> solrDocuments,
			Locale locale, String token, int weight)
		throws SearchException {

		int length = token.length();

		SolrInputDocument solrInputDocument = new SolrInputDocument();

		StringBundler sb = new StringBundler(6);
		sb.append("spellcheck");
		sb.append(StringPool.UNDERLINE);
		sb.append(token);
		sb.append(StringPool.UNDERLINE);
		sb.append(locale.toString());

		solrInputDocument.addField("uid", sb.toString());
		solrInputDocument.addField("spellcheck", true);
		solrInputDocument.addField("word", token);
		solrInputDocument.addField("weight", String.valueOf(weight));
		solrInputDocument.addField("locale", locale.toString());

		addGram(solrInputDocument, token, getMin(length), getMax(length));

		solrDocuments.add(solrInputDocument);
	}

	private void addGram(
			SolrInputDocument solrInputDocument, String text, int ng1, int ng2)
		throws SearchException {

		Map<String, Object> nGramsMap = buildNGrams(text, ng1, ng2);

		for (Map.Entry entry : nGramsMap.entrySet()) {
			String key = (String)entry.getKey();

			if (entry.getValue() instanceof String) {
				solrInputDocument.addField(key, entry.getValue());
			}

			else if (entry.getValue() instanceof List) {
				List<String> ngrams = (List)entry.getValue();

				for (String ngram : ngrams) {
					solrInputDocument.addField(key, ngram);
				}
			}

		}
	}

	private void doIndexDictionary(File file, Locale locale)
		throws SearchException {

		Set<SolrInputDocument> docs = new HashSet<SolrInputDocument>();

		try {
			BufferedReader in = new BufferedReader(new FileReader(file));
			String line;
			int i = 0;
			while ((line = in.readLine()) != null) {
				i++;
				String[] term = line.split(StringPool.SPACE);
				int weight = 0;
				if (term.length > 1) {
					weight = Integer.parseInt(term[1]);
				}

				addDocument(docs, locale, term[0], weight);
				if (i == BATCH_NUM) {
					_solrServer.add(docs);
					if (_commit) {
						_solrServer.commit();
					}

					docs.clear();
					i = 0;
				}
			}

			if (i != 0) {
				_solrServer.add(docs);
				if (_commit) {
					_solrServer.commit();
				}
			}

			in.close();
		}
		catch (Exception e) {
			if (_log.isDebugEnabled()) {
				_log.debug("Unable to execute Solr query", e);
			}

			throw new SearchException(e.getMessage());
		}
	}

	private static final int BATCH_NUM = 10000;

	private static final String CUSTOM_EXTENSION_FILE =".txt";
	private static final String CUSTOM_PREFIX ="custom";
	private static final String DICTIONARY_DIRECTORY = "dictionaries.directory";
	private static final String DICTIONARY_EXTENSION_FILE =".txt";

	private static Log _log = LogFactoryUtil.getLog(
		SolrSpellCheckIndexWriterImpl.class);

	private boolean _commit;

	private SolrServer _solrServer;
	private Set<String> _supportedLocales;

}