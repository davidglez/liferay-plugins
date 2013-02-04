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
import java.util.Locale;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;

/**
 * @author Michael C. Han
 */
public class SolrSpellCheckIndexWriterImpl implements SpellCheckIndexWriter {

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

	public void doIndexDictionary(File file, Locale locale)
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
				if (i == _BATCH_NUM) {
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

		String dictionaryDir = PortletProps.get(_DICTIONARY_DIRECTORY);
		String dictionaryRelativePath = dictionaryDir.concat(
			strLocale).concat(_DICTIONARY_EXTENSION_FILE);
		String dictionaryCompletePath =
			SolrSpellCheckIndexWriterImpl.class.getClassLoader()
				.getResource(dictionaryRelativePath).getFile();

		File fileExternal = new File(dictionaryCompletePath);

		doIndexDictionary(fileExternal, locale);

		String customDir = PortletProps.get(_DICTIONARY_DIRECTORY);

		StringBundler customRelativePath = new StringBundler(5);

		customRelativePath.append(customDir);
		customRelativePath.append(_CUSTOM_PREFIX);
		customRelativePath.append(StringPool.UNDERLINE);
		customRelativePath.append(strLocale);
		customRelativePath.append(_CUSTOM_EXTENSION_FILE);

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

	protected static void addDocument(
		Set<SolrInputDocument> solrDocuments, Locale locale, String token,
		int weight) {

		int length = token.length();

		SolrInputDocument solrInputDocument = new SolrInputDocument();

		solrInputDocument.addField(
			"uid", "spellcheck" + token + locale.toString());

		solrInputDocument.addField("spellcheck", true);
		solrInputDocument.addField("word", token);
		solrInputDocument.addField("weight", String.valueOf(weight));

		if (locale != null) {
			solrInputDocument.addField("locale", locale.toString());
		}

		addGram(solrInputDocument, token, getMin(length), getMax(length));

		solrDocuments.add(solrInputDocument);
	}

	private static void addGram(
		SolrInputDocument solrInputDocument, String text, int ng1, int ng2) {

		int len = text.length();

		for (int ng = ng1; ng <= ng2; ng++) {

			String key = "gram" + ng;
			String end = null;

			for (int i = 0; i < len - ng + 1; i++) {

				String gram = text.substring(i, i + ng);
				solrInputDocument.addField(key, gram);
				if (i == 0) {

					solrInputDocument.addField("start" + ng, gram);
				}

				end = gram;
			}

			if (end != null) {

				solrInputDocument.addField("end" + ng, end);
			}
		}
	}

	private static int getMax(int l) {
		if (l > 5) {
			return 4;
		}
		else {
			return 3;
		}
	}

	private static int getMin(int l) {
		if (l > 5) {
			return 3;
		}
		else {
			return 2;
		}
	}

	private static final int _BATCH_NUM = 1000;

	private static final String _CUSTOM_EXTENSION_FILE =".txt";

	private static final String _CUSTOM_PREFIX ="custom";

	private static final String _DICTIONARY_DIRECTORY = "dictionary.directory";

	private static final String _DICTIONARY_EXTENSION_FILE =".txt";

	private static Log _log = LogFactoryUtil.getLog(
		SolrSpellCheckIndexWriterImpl.class);

	private boolean _commit;

	private SolrServer _solrServer;
	private Set<String> _supportedLocales;

}