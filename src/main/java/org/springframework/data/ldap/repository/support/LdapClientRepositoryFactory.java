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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Optional;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.ldap.core.mapping.LdapMappingContext;
import org.springframework.data.ldap.repository.query.AnnotatedLdapClientRepositoryQuery;
import org.springframework.data.ldap.repository.query.LdapQueryMethod;
import org.springframework.data.ldap.repository.query.PartTreeLdapClientRepositoryQuery;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.lang.Nullable;
import org.springframework.ldap.core.LdapMapperClient;
import org.springframework.ldap.odm.core.ObjectDirectoryMapper;
import org.springframework.util.Assert;

import static org.springframework.data.querydsl.QuerydslUtils.QUERY_DSL_PRESENT;
import static org.springframework.data.repository.core.support.RepositoryComposition.RepositoryFragments;

/**
 * Factory to create {@link org.springframework.data.ldap.repository.LdapRepository} instances.
 *
 * @author Mattias Hellborg Arthursson
 * @author Eddu Melendez
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class LdapClientRepositoryFactory extends RepositoryFactorySupport {

	private final LdapClientQueryLookupStrategy queryLookupStrategy;
	private final LdapMapperClient ldap;
	private final MappingContext<? extends PersistentEntity<?, ?>, ? extends PersistentProperty<?>> mappingContext;
	private final EntityInstantiators instantiators = new EntityInstantiators();

	/**
	 * Creates a new {@link LdapClientRepositoryFactory}.
	 *
	 * @param ldap must not be {@literal null}.
	 */
	public LdapClientRepositoryFactory(LdapMapperClient ldap) {

		Assert.notNull(ldap, "LdapClient must not be null");

		this.ldap = ldap;
		this.mappingContext = new LdapMappingContext();
		this.queryLookupStrategy = new LdapClientQueryLookupStrategy(ldap, instantiators, mappingContext);
	}

	/**
	 * Creates a new {@link LdapClientRepositoryFactory}.
	 *
	 * @param ldap must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	LdapClientRepositoryFactory(LdapMapperClient ldap,
								MappingContext<? extends PersistentEntity<?, ?>, ? extends PersistentProperty<?>> mappingContext) {

		Assert.notNull(ldap, "LdapClient must not be null");
		Assert.notNull(mappingContext, "LdapMappingContext must not be null");

		this.queryLookupStrategy = new LdapClientQueryLookupStrategy(ldap, instantiators, mappingContext);
		this.ldap = ldap;
		this.mappingContext = mappingContext;
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <T, ID> EntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
		return new LdapEntityInformation(domainClass, ldap);
	}

	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleLdapRepository.class;
	}

	@Override
	protected RepositoryFragments getRepositoryFragments(RepositoryMetadata metadata) {
		return getRepositoryFragments(metadata, ldap);
	}

	/**
	 * Creates {@link RepositoryFragments} based on {@link RepositoryMetadata} to add LDAP-specific extensions. Typically,
	 * adds a {@link QuerydslLdapPredicateExecutor} if the repository interface uses Querydsl.
	 * <p>
	 * Can be overridden by subclasses to customize {@link RepositoryFragments}.
	 *
	 * @param metadata repository metadata.
	 * @param ldap the LDAP client.
	 * @return
	 * @since 2.6
	 */
	protected RepositoryFragments getRepositoryFragments(RepositoryMetadata metadata, LdapMapperClient ldap) {

		boolean isQueryDslRepository = QUERY_DSL_PRESENT
				&& QuerydslPredicateExecutor.class.isAssignableFrom(metadata.getRepositoryInterface());

		if (isQueryDslRepository) {

			if (metadata.isReactiveRepository()) {
				throw new InvalidDataAccessApiUsageException(
						"Cannot combine Querydsl and reactive repository support in a single interface");
			}

			return RepositoryFragments.just(new QuerydslLdapClientPredicateExecutor<>(
					getEntityInformation(metadata.getDomainType()), getProjectionFactory(), ldap, mappingContext));
		}

		return RepositoryFragments.empty();
	}

	@Override
	protected Object getTargetRepository(RepositoryInformation information) {

		boolean acceptsMappingContext = acceptsMappingContext(information);

		if (acceptsMappingContext) {
			return getTargetRepositoryViaReflection(information, ldap, mappingContext);
		}

		return getTargetRepositoryViaReflection(information, ldap);
	}

	@Override
	protected Optional<QueryLookupStrategy> getQueryLookupStrategy(@Nullable Key key,
			QueryMethodEvaluationContextProvider evaluationContextProvider) {
		return Optional.of(queryLookupStrategy);
	}

	/**
	 * Allow creation of repository base classes that do not accept a {@link LdapMappingContext} that was introduced with
	 * version 2.6.
	 *
	 * @param information
	 * @return
	 */
	private static boolean acceptsMappingContext(RepositoryInformation information) {

		Class<?> repositoryBaseClass = information.getRepositoryBaseClass();

		Constructor<?>[] declaredConstructors = repositoryBaseClass.getDeclaredConstructors();

		boolean acceptsMappingContext = false;

		for (Constructor<?> declaredConstructor : declaredConstructors) {
			Class<?>[] parameterTypes = declaredConstructor.getParameterTypes();

			if (parameterTypes.length == 4 && parameterTypes[1].isAssignableFrom(LdapMappingContext.class)) {
				acceptsMappingContext = true;
			}
		}

		return acceptsMappingContext;
	}

	private static final class LdapClientQueryLookupStrategy implements QueryLookupStrategy {

		private final LdapMapperClient ldap;
		private final EntityInstantiators instantiators;
		private final MappingContext<? extends PersistentEntity<?, ?>, ? extends PersistentProperty<?>> mappingContext;

		public LdapClientQueryLookupStrategy(LdapMapperClient ldap, EntityInstantiators instantiators,
											 MappingContext<? extends PersistentEntity<?, ?>, ? extends PersistentProperty<?>> mappingContext) {

			this.ldap = ldap;
			this.instantiators = instantiators;
			this.mappingContext = mappingContext;
		}

		@Override
		public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, ProjectionFactory factory,
				NamedQueries namedQueries) {

			LdapQueryMethod queryMethod = new LdapQueryMethod(method, metadata, factory);
			Class<?> domainType = metadata.getDomainType();
			if (queryMethod.hasQueryAnnotation()) {
				return new AnnotatedLdapClientRepositoryQuery(queryMethod, domainType, ldap, mappingContext, instantiators);
			} else {
				return new PartTreeLdapClientRepositoryQuery(queryMethod, domainType, ldap, mappingContext, instantiators);
			}
		}
	}
}
