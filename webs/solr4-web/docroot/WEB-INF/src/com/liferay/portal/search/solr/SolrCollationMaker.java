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

import com.liferay.portal.kernel.util.StringPool;

import java.util.List;
import java.util.Map;

/**
 * @author Daniela Zapata
 * @author David Gonzalez
 */
public class SolrCollationMaker implements CollationMaker {

	public String createCollation(
		Map<String, List<String>> mapSuggestions, List<String> tokens) {

		String collated = StringPool.BLANK;

		for (String token : tokens) {
			if (!mapSuggestions.get(token).isEmpty()) {

				String suggestion = mapSuggestions.get(token).get(0);

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
}
