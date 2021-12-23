package org.apache.cayenne.dbsync.merge.token.model;

import java.sql.Types;
import java.util.Collection;
import java.util.HashSet;

import org.apache.cayenne.dbsync.merge.MergeCase;
import org.apache.cayenne.dbsync.merge.token.MergerToken;
import org.apache.cayenne.map.DbAttribute;
import org.apache.cayenne.map.DbEntity;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SetPrimaryKeyToModelTest extends MergeCase {

    @Test
    public void testSetPKFlag() throws Exception {
        dropTableIfPresent("NEW_TABLE");
        assertTokensAndExecute(0, 0);

        DbEntity dbEntity = new DbEntity("NEW_TABLE");
        assertNotNull(dbEntity);

        DbAttribute attribute = new DbAttribute("ID1", Types.INTEGER, dbEntity);
        assertNotNull(attribute);
        assertFalse(attribute.isPrimaryKey());

        attribute.setMandatory(true);
        attribute.setPrimaryKey(true);
        assertTrue(attribute.isPrimaryKey());
        dbEntity.addAttribute(attribute);
        map.addDbEntity(dbEntity);
        //There are added entity. One token for creating table in DB.

        assertTokensAndExecute(1, 0);
        assertTokensAndExecute(0, 0);
        assertTrue(attribute.isPrimaryKey());

        Collection<DbAttribute> primaryKeyOriginal = new HashSet<>();
        primaryKeyOriginal.add(attribute);

        Collection<DbAttribute> primaryKeyNew = new HashSet<>();

        MergerToken token = mergerFactory().createSetPrimaryKeyToModel(dbEntity, primaryKeyOriginal, primaryKeyNew,"id1_pk");
        execute(token);

        assertFalse(attribute.isPrimaryKey());
    }


    @Test
    public void testChangePK() throws Exception {
        dropTableIfPresent("NEW_TABLE");
        assertTokensAndExecute(0, 0);

        DbEntity dbEntity = new DbEntity("NEW_TABLE");
        assertNotNull(dbEntity);

        DbAttribute attribute1 = new DbAttribute("ID1", Types.INTEGER, dbEntity);
        assertNotNull(attribute1);
        assertFalse(attribute1.isPrimaryKey());

        DbAttribute attribute2 = new DbAttribute("ID2", Types.INTEGER, dbEntity);
        assertNotNull(attribute2);
        assertFalse(attribute2.isPrimaryKey());

        attribute1.setMandatory(true);
        attribute1.setPrimaryKey(true);
        assertTrue(attribute1.isPrimaryKey());

        dbEntity.addAttribute(attribute1);
        dbEntity.addAttribute(attribute2);
        map.addDbEntity(dbEntity);
        //There are added entity. One token for creating table in DB.

        assertTokensAndExecute(1, 0);
        assertTokensAndExecute(0, 0);
        assertTrue(attribute1.isPrimaryKey());
        assertFalse(attribute2.isPrimaryKey());

        //Change PK.
        Collection<DbAttribute> primaryKeyOriginal = new HashSet<>();
        primaryKeyOriginal.add(attribute1);

        Collection<DbAttribute> primaryKeyNew = new HashSet<>();
        primaryKeyNew.add(attribute2);

        MergerToken token = mergerFactory().createSetPrimaryKeyToModel(dbEntity, primaryKeyOriginal, primaryKeyNew, "id2_pk");
        execute(token);

        assertTrue(attribute2.isPrimaryKey());
        assertFalse(attribute1.isPrimaryKey());
    }
}