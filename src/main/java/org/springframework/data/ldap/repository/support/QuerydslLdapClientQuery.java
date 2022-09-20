/*
 * Copyright 2016-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.ldap.repository.support;

import java.util.List;
import java.util.function.Consumer;

import com.querydsl.core.DefaultQueryMetadata;
import com.querydsl.core.FilteredClause;
import com.querydsl.core.support.QueryMixin;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.Predicate;

import org.springframework.ldap.core.LdapMapperClient;
import org.springframework.ldap.filter.AbsoluteTrueFilter;
import org.springframework.ldap.query.LdapQueryBuilder;
import org.springframework.util.Assert;

/**
 * Spring LDAP specific {@link FilteredClause} implementation.
 *
 * @author Mattias Hellborg Arthursson
 * @author Eddu Melendez
 * @author Mark Paluch
 */
public class QuerydslLdapClientQuery<K> implements FilteredClause<QuerydslLdapClientQuery<K>> {

	private final LdapMapperClient ldap;
	private final Class<K> entityType;
	private final LdapSerializer filterGenerator;
	private final Consumer<LdapQueryBuilder> queryCustomizer;

	private QueryMixin<QuerydslLdapClientQuery<K>> queryMixin = new QueryMixin<>(this, new DefaultQueryMetadata().noValidate());

	/**
	 * Creates a new {@link QuerydslLdapClientQuery}.
	 *
	 * @param ldap must not be {@literal null}.
	 * @param entityPath must not be {@literal null}.
	 */
	public QuerydslLdapClientQuery(LdapMapperClient ldap, EntityPath<K> entityPath, LdapSerializer ldapSerializer) {
		this(ldap, (Class<K>) entityPath.getType(), ldapSerializer);
	}

	/**
	 * Creates a new {@link QuerydslLdapClientQuery}.
	 *
	 * @param ldap must not be {@literal null}.
	 * @param entityType must not be {@literal null}.
	 */
	public QuerydslLdapClientQuery(LdapMapperClient ldap, Class<K> entityType, LdapSerializer ldapSerializer) {
		this(ldap, entityType, it -> {

		}, ldapSerializer);
	}

	/**
	 * Creates a new {@link QuerydslLdapClientQuery}.
	 *
	 * @param ldap must not be {@literal null}.
	 * @param entityType must not be {@literal null}.
	 * @param queryCustomizer must not be {@literal null}.
	 * @since 2.6
	 */
	public QuerydslLdapClientQuery(LdapMapperClient ldap, Class<K> entityType,
								   Consumer<LdapQueryBuilder> queryCustomizer, LdapSerializer ldapSerializer) {

		Assert.notNull(ldap, "LdapClient must not be null");
		Assert.notNull(entityType, "Type must not be null");
		Assert.notNull(queryCustomizer, "Query customizer must not be null");

		this.ldap = ldap;
		this.entityType = entityType;
		this.queryCustomizer = queryCustomizer;
		this.filterGenerator = ldapSerializer;
	}

	@Override
	public QuerydslLdapClientQuery<K> where(Predicate... o) {

		if (o == null) {
			return this;
		}

		return queryMixin.where(o);
	}

	@SuppressWarnings("unchecked")
	public List<K> list() {
		return ldap.search(entityType).query(buildQuery()).toList();
	}

	public K uniqueResult() {
		return ldap.search(entityType).query(buildQuery()).toObject();
	}

	Consumer<LdapQueryBuilder> buildQuery() {
		return (builder) -> {
			Predicate where = queryMixin.getMetadata().getWhere();

			queryCustomizer.accept(builder);

			if (where != null) {
				builder.filter(filterGenerator.handle(where));
			} else {
				builder.filter(new AbsoluteTrueFilter());
			}
		};
	}
}
