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
package org.springframework.data.ldap.repository.query;

import java.util.function.Consumer;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.ldap.repository.Query;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.ldap.core.LdapMapperClient;
import org.springframework.ldap.core.LdapOperations;
import org.springframework.ldap.query.LdapQuery;
import org.springframework.ldap.query.LdapQueryBuilder;
import org.springframework.util.Assert;

import static org.springframework.data.ldap.repository.query.LdapClientQueryExecution.CollectionExecution;
import static org.springframework.data.ldap.repository.query.LdapClientQueryExecution.FindOneExecution;
import static org.springframework.data.ldap.repository.query.LdapClientQueryExecution.ResultProcessingConverter;
import static org.springframework.data.ldap.repository.query.LdapClientQueryExecution.ResultProcessingExecution;
import static org.springframework.data.ldap.repository.query.LdapClientQueryExecution.StreamExecution;

/**
 * Base class for {@link RepositoryQuery} implementations for LDAP.
 *
 * @author Mattias Hellborg Arthursson
 * @author Mark Paluch
 */
public abstract class AbstractLdapClientRepositoryQuery implements RepositoryQuery {

	private final LdapQueryMethod queryMethod;
	private final Class<?> entityType;
	private final LdapMapperClient ldap;
	private final MappingContext<? extends PersistentEntity<?, ?>, ? extends PersistentProperty<?>> mappingContext;
	private final EntityInstantiators instantiators;

	/**
	 * Creates a new {@link AbstractLdapClientRepositoryQuery} instance given {@link LdapQuery}, {@link Class} and
	 * {@link LdapOperations}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param entityType must not be {@literal null}.
	 * @param ldap must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 * @param instantiators must not be {@literal null}.
	 */
	public AbstractLdapClientRepositoryQuery(LdapQueryMethod queryMethod, Class<?> entityType, LdapMapperClient ldap,
											 MappingContext<? extends PersistentEntity<?, ?>, ? extends PersistentProperty<?>> mappingContext,
											 EntityInstantiators instantiators) {

		Assert.notNull(queryMethod, "LdapQueryMethod must not be null");
		Assert.notNull(entityType, "Entity type must not be null");
		Assert.notNull(ldap, "LdapRepository must not be null");

		this.queryMethod = queryMethod;
		this.entityType = entityType;
		this.ldap = ldap;
		this.mappingContext = mappingContext;
		this.instantiators = instantiators;
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public final Object execute(Object[] parameters) {

		LdapParametersParameterAccessor parameterAccessor = new LdapParametersParameterAccessor(queryMethod, parameters);
		Consumer<LdapQueryBuilder> query = createQuery(parameterAccessor);

		ResultProcessor processor = queryMethod.getResultProcessor().withDynamicProjection(parameterAccessor);

		ResultProcessingConverter converter = new ResultProcessingConverter(processor, mappingContext, instantiators);
		ResultProcessingExecution execution = new ResultProcessingExecution(
				getLdapQueryExecutionToWrap(converter), converter);

		return execution.execute(query);
	}

	private LdapClientQueryExecution getLdapQueryExecutionToWrap(
			Converter<Object, Object> resultProcessing) {

		if (queryMethod.isCollectionQuery()) {
			return new CollectionExecution(ldap, entityType);
		} else if (queryMethod.isStreamQuery()) {
			return new StreamExecution(ldap, entityType, resultProcessing);
		} else {
			return new FindOneExecution(ldap, entityType);
		}
	}

	/**
	 * Creates a {@link Query} instance using the given {@literal parameters}.
	 *
	 * @param parameters must not be {@literal null}.
	 * @return
	 */
	protected abstract Consumer<LdapQueryBuilder> createQuery(LdapParameterAccessor parameters);

	/**
	 * @return
	 */
	protected Class<?> getEntityClass() {
		return entityType;
	}

	@Override
	public final QueryMethod getQueryMethod() {
		return queryMethod;
	}
}
