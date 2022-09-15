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
package org.springframework.data.ldap.repository;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;

import javax.naming.Name;
import javax.naming.ldap.LdapName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.stubbing.Answer;

import org.springframework.LdapDataEntry;
import org.springframework.data.domain.Persistable;
import org.springframework.data.ldap.repository.support.SimpleLdapClientRepository;
import org.springframework.data.ldap.repository.support.SimpleLdapRepository;
import org.springframework.ldap.NameNotFoundException;
import org.springframework.ldap.core.ContextMapper;
import org.springframework.ldap.core.DirContextAdapter;
import org.springframework.ldap.core.LdapClient;
import org.springframework.ldap.core.LdapMapperClient;
import org.springframework.ldap.core.support.CountNameClassPairCallbackHandler;
import org.springframework.ldap.filter.Filter;
import org.springframework.ldap.odm.core.ObjectDirectoryMapper;
import org.springframework.ldap.query.LdapQuery;
import org.springframework.ldap.support.LdapUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SimpleLdapRepository}.
 *
 * @author Mattias Hellborg Arthursson
 * @author Mark Paluch
 * @author Jens Schauder
 */
@MockitoSettings
class SimpleLdapClientRepositoryTests {

	LdapMapperClient<Object> ldap = new MockLdapClient();

	@Mock ObjectDirectoryMapper odm;

	private SimpleLdapClientRepository<Object> tested;

	@BeforeEach
	void prepareTestedInstance() {
		tested = new SimpleLdapClientRepository<>(ldap);
	}

	@Test
	void testCount() {

		Filter filterMock = mock(Filter.class);
		when(odm.filterFor(Object.class, null)).thenReturn(filterMock);
		ArgumentCaptor<LdapQuery> ldapQuery = ArgumentCaptor.forClass(LdapQuery.class);
		LdapClient.SearchSpec search = ldap.search();
		given(search.query(ldapQuery.capture())).willReturn(search);
		doNothing().when(search).handle(any(CountNameClassPairCallbackHandler.class));

		long count = tested.count();

		assertThat(count).isEqualTo(0);
		LdapQuery query = ldapQuery.getValue();
		assertThat(query.filter()).isEqualTo(filterMock);
		assertThat(query.attributes()).isEqualTo(new String[] { "objectclass" });
	}

	@Test
	void testSaveNonPersistableWithIdSet() {

		Object expectedEntity = new Object();

		Name id = LdapUtils.emptyLdapName();
		when(odm.getId(expectedEntity)).thenReturn(id);
		when(odm.getCalculatedId(expectedEntity)).thenReturn(id);
		given(ldap.search().toObject(any(ContextMapper.class))).willReturn(new DirContextAdapter(id));

		tested.save(expectedEntity);

		verify(ldap.bind(id)).execute();
	}

	@Test
	void testSaveNonPersistableWithIdChanged() {

		Object expectedEntity = new Object();
		LdapName id = LdapUtils.emptyLdapName();
		LdapName newId = LdapUtils.newLdapName("ou=newlocation");

		when(odm.getId(expectedEntity)).thenReturn(id);
		when(odm.getCalculatedId(expectedEntity)).thenReturn(newId);
		given(ldap.search().toObject(any(ContextMapper.class))).willReturn(new DirContextAdapter(id));

		tested.save(expectedEntity);

		verify(ldap.unbind(id)).execute();
		verify(ldap.bind(newId)).execute();
	}

	@Test
	void testSaveNonPersistableWithNoIdCalculatedId() {

		Object expectedEntity = new Object();
		LdapName calculated = LdapUtils.newLdapName("ou=newlocation");
		when(odm.getCalculatedId(expectedEntity)).thenReturn(calculated);

		tested.save(expectedEntity);

		verify(ldap.bind(calculated)).execute();
	}

	@Test
	void testSavePersistableNewWithDeclaredId() {

		Persistable<?> expectedEntity = mock(Persistable.class);
		LdapName id = LdapUtils.emptyLdapName();
		when(expectedEntity.isNew()).thenReturn(true);
		when(odm.getId(expectedEntity)).thenReturn(id);

		tested.save(expectedEntity);

		verify(ldap.bind(id)).execute();
	}

	@Test
	void testSavePersistableNewWithCalculatedId() {

		Persistable<?> expectedEntity = mock(Persistable.class);
		LdapName expectedName = LdapUtils.emptyLdapName();

		when(expectedEntity.isNew()).thenReturn(true);
		when(odm.getCalculatedId(expectedEntity)).thenReturn(expectedName);

		tested.save(expectedEntity);

		verify(ldap.bind(expectedName)).execute();
	}

	@Test
	void testSavePersistableNotNew() {

		Persistable<?> expectedEntity = mock(Persistable.class);
		LdapName id = LdapUtils.emptyLdapName();
		when(expectedEntity.isNew()).thenReturn(false);
		when(odm.getId(expectedEntity)).thenReturn(id);

		tested.save(expectedEntity);

		verify(ldap.bind(id)).execute();
		verify(ldap.bind(id)).replaceExisting(true);
	}

	@Test
	void testFindOneWithName() {

		LdapName expectedName = LdapUtils.emptyLdapName();
		Object expectedResult = new Object();

		when(ldap.search().toObject(any(ContextMapper.class))).thenReturn(expectedResult);

		Optional<Object> actualResult = tested.findById(expectedName);

		assertThat(actualResult).contains(expectedResult);
	}

	@Test // DATALDAP-21
	void verifyThatNameNotFoundInFindOneWithNameReturnsEmptyOptional() {

		LdapName expectedName = LdapUtils.emptyLdapName();

		when(ldap.search().toObject(any(ContextMapper.class))).thenThrow(new NameNotFoundException(""));

		Optional<Object> actualResult = tested.findById(expectedName);

		assertThat(actualResult).isNotPresent();
	}

	@Test // DATALDAP-21
	void verifyThatNoResultFoundInFindOneWithNameReturnsEmptyOptional() {

		LdapName expectedName = LdapUtils.emptyLdapName();

		when(ldap.search().toObject(any(ContextMapper.class))).thenReturn(null);

		Optional<Object> actualResult = tested.findById(expectedName);

		assertThat(actualResult).isNotPresent();
	}

	@Test
	void testFindAll() {

		Name expectedName1 = LdapUtils.newLdapName("ou=aa");
		Name expectedName2 = LdapUtils.newLdapName("ou=bb");

		Object expectedResult1 = new Object();
		Object expectedResult2 = new Object();
		Iterator<Object> expected = Arrays.asList(expectedResult1, expectedResult2).iterator();
		when(ldap.search().toObject(any(ContextMapper.class))).thenAnswer((invocation) -> expected.next());

		Iterable<Object> actualResult = tested.findAllById(Arrays.asList(expectedName1, expectedName2));

		Iterator<Object> iterator = actualResult.iterator();
		assertThat(iterator.next()).isSameAs(expectedResult1);
		assertThat(iterator.next()).isSameAs(expectedResult2);

		assertThat(iterator.hasNext()).isFalse();
	}

	@Test
	void testFindAllWhereOneEntryIsNotFound() {

		Name expectedName1 = LdapUtils.newLdapName("ou=aa");
		Name expectedName2 = LdapUtils.newLdapName("ou=bb");

		Object expectedResult2 = new Object();
		Iterator<Object> expected = Arrays.asList(null, expectedResult2).iterator();
		when(ldap.search().toObject(any(ContextMapper.class))).thenAnswer((invocation) -> expected.next());

		Iterable<Object> actualResult = tested.findAllById(Arrays.asList(expectedName1, expectedName2));

		Iterator<Object> iterator = actualResult.iterator();
		assertThat(iterator.next()).isSameAs(expectedResult2);

		assertThat(iterator.hasNext()).isFalse();
	}

	private static class MockLdapClient implements LdapMapperClient<Object> {

		private final ListSpec list = mock(ListSpec.class, RETURNS_SELF);
		private final ListBindingsSpec listBindings = mock(ListBindingsSpec.class, RETURNS_SELF);
		private final MapperSearchSpec<Object> search = mock(MapperSearchSpec.class, RETURNS_SELF);
		private final AuthenticateSpec authenticate = mock(AuthenticateSpec.class, RETURNS_SELF);
		private final BindSpec bind = mock(BindSpec.class, RETURNS_SELF);
		private final UnbindSpec unbind = mock(UnbindSpec.class, RETURNS_SELF);
		private final ModifySpec modify = mock(ModifySpec.class, RETURNS_SELF);

		@Override
		public ListSpec list(String name) {
			return list;
		}

		@Override
		public ListSpec list(Name name) {
			return list;
		}

		@Override
		public ListBindingsSpec listBindings(String name) {
			return listBindings;
		}

		@Override
		public ListBindingsSpec listBindings(Name name) {
			return listBindings;
		}

		@Override
		public MapperSearchSpec<Object> search() {
			return search;
		}

		@Override
		public AuthenticateSpec authenticate() {
			return authenticate;
		}

		@Override
		public BindSpec bind(String name) {
			return bind;
		}

		@Override
		public BindSpec bind(Name name) {
			return bind;
		}

		@Override
		public ModifySpec modify(String name) {
			return modify;
		}

		@Override
		public ModifySpec modify(Name name) {
			return modify;
		}

		@Override
		public UnbindSpec unbind(String name) {
			return unbind;
		}

		@Override
		public UnbindSpec unbind(Name name) {
			return unbind;
		}

		@Override
		public Builder mutate() {
			return null;
		}

		@Override
		public <S> S create(S entity) {
			return null;
		}

		@Override
		public <S> S update(S entity) {
			return null;
		}

		@Override
		public void delete(Object entity) {

		}
	}
}
