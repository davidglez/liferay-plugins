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

import java.util.Comparator;
import java.util.Map;

/**
 * @author Daniela Zapata
 * @author David Gonzalez
 */
public class ValueComparator implements Comparator<String> {

	public ValueComparator(Map<String, Float> base) {
		_base = base;
	}

	public int compare(String a, String b) {
		if (_base.get(a) >= _base.get(b)) {
			return -1;
		}
		else {
			return 1;
		}
	}

	Map<String, Float> _base;

}
