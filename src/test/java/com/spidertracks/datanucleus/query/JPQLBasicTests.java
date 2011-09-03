/**********************************************************************
Copyright (c) 2010 Pulasthi Supun. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
...
***********************************************************************/
package com.spidertracks.datanucleus.query;

import static org.junit.Assert.assertEquals;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.spidertracks.datanucleus.CassandraTest;

import com.spidertracks.datanucleus.basic.model.Person;
import com.spidertracks.datanucleus.basic.model.PrimitiveObject;

public class JPQLBasicTests extends CassandraTest
{

    Object[] id = new Object[3];

    private PrimitiveObject object1;
    private PrimitiveObject object2;
    private PrimitiveObject object3;
    private PersistenceManager setupPm;

    private Person p1;
    private Person p2;
    private Person p3;
    private Person p4;
    private Person p5;

    @Before
    public void setUp() throws Exception {
        

        setupPm = pmf.getPersistenceManager();

        Transaction tx = setupPm.currentTransaction();
        tx.begin();

        object1 = new PrimitiveObject();
        object1.setTestByte((byte) 0xf1);
        object1.setTestBool(true);
        object1.setTestChar('1');
        object1.setTestDouble(100.10);
        object1.setTestFloat((float) 100.10);
        object1.setTestInt(10);
        object1.setTestLong(100);
        object1.setTestShort((short) 1);
        object1.setTestString("one");
        object1.setNonIndexedString("none");

        setupPm.makePersistent(object1);

        object2 = new PrimitiveObject();
        object2.setTestByte((byte) 0xf1);
        object2.setTestBool(true);
        object2.setTestChar('2');
        object2.setTestDouble(200.20);
        object2.setTestFloat((float) 200.20);
        object2.setTestInt(20);
        object2.setTestLong(200);
        object2.setTestShort((short) 2);
        object2.setTestString("two");
        object2.setNonIndexedString("ntwo");

        setupPm.makePersistent(object2);

        object3 = new PrimitiveObject();
        object3.setTestByte((byte) 0xf1);
        object3.setTestBool(true);
        object3.setTestChar('3');
        object3.setTestDouble(300.30);
        object3.setTestFloat((float) 300.30);
        object3.setTestInt(30);
        object3.setTestLong(300);
        object3.setTestShort((short) 3);
        object3.setTestString("three");
        object3.setNonIndexedString("nthree");

        setupPm.makePersistent(object3);

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -20);

        p1 = new Person();
        p1.setEmail("p1@test.com");
        p1.setFirstName("firstName1");
        p1.setLastName("lastName1");
        p1.setLastLogin(cal.getTime());

        cal.add(Calendar.DATE, 2);

        p2 = new Person();
        p2.setEmail("p2@test.com");
        p2.setFirstName("firstName1");
        p2.setLastName("secondName1");
        p2.setLastLogin(cal.getTime());

        cal.add(Calendar.DATE, 2);

        p3 = new Person();
        p3.setEmail("p3@test.com");
        p3.setFirstName("firstName1");
        p3.setLastName("secondName2");
        p3.setLastLogin(cal.getTime());

        cal.add(Calendar.DATE, 2);

        p4 = new Person();
        p4.setEmail("p4@test.com");
        p4.setFirstName("firstName2");
        p4.setLastName("secondName2");
        p4.setLastLogin(cal.getTime());

        cal.add(Calendar.DATE, 2);

        p5 = new Person();
        p5.setEmail("p5@test.com");
        p5.setFirstName("firstName3");
        p5.setLastName("secondName3");
        p5.setLastLogin(cal.getTime());

        // now persist everything

        setupPm.makePersistent(p1);
        setupPm.makePersistent(p2);
        setupPm.makePersistent(p3);
        setupPm.makePersistent(p4);
        setupPm.makePersistent(p5);

        tx.commit();

    }

    @After
    public void tearDown() throws Exception {
        Transaction tx = setupPm.currentTransaction();
        tx.begin();

        setupPm.deletePersistent(object1);
        setupPm.deletePersistent(object2);
        setupPm.deletePersistent(object3);

        setupPm.deletePersistent(p1);
        setupPm.deletePersistent(p2);
        setupPm.deletePersistent(p3);
        setupPm.deletePersistent(p4);
        setupPm.deletePersistent(p5);

        tx.commit();

    }

    /**
     * basic query JPQL
     */
     @SuppressWarnings("rawtypes")
     @Test
     public void testQueryJPQL() {
         PersistenceManager pm = pmf.getPersistenceManager();
         Transaction tx = pm.currentTransaction();

         try {
             tx.begin();
             Query q = pm.newQuery("javax.jdo.query.JPQL", "SELECT doc FROM  com.spidertracks.datanucleus." 
                     + "basic.model.PrimitiveObject as doc ");
             Collection c = (Collection) q.execute();
             assertEquals(3, c.size());
             tx.commit();

         } finally {
             if (tx.isActive()) {
                 tx.rollback();
             }
         pm.close();
         }
     }

    /**
     * non index result test for JPQL
     */
     @SuppressWarnings("rawtypes")
     @Test
     public void testFilterJPQL() {
         PersistenceManager pm = pmf.getPersistenceManager();
         Transaction tx = pm.currentTransaction();

         try {
             tx.begin();
             Query q = pm.newQuery("javax.jdo.query.JPQL", "SELECT doc FROM  com.spidertracks.datanucleus." 
                     + "basic.model.PrimitiveObject as doc WHERE doc.testString = 'one' ");
             Collection c = (Collection) q.execute();
             assertEquals(1, c.size());
             Iterator it = c.iterator();
             assertEquals("one", ((PrimitiveObject) it.next()).getTestString());
             tx.commit();

         } finally {
             if (tx.isActive()) {
                 tx.rollback();
             }
         pm.close();
         }
     }

    /**
     * non index result test for JPQL
     */
    @SuppressWarnings("rawtypes")
    @Test
    public void testFilterNonIndexedJPQL() {
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();

        try {
            tx.begin();
            Query q = pm
                    .newQuery(
                            "javax.jdo.query.JPQL",
                            "SELECT doc FROM  com.spidertracks.datanucleus."
                                    + "basic.model.PrimitiveObject as doc WHERE doc.nonIndexedString = 'none'");
            Collection c = (Collection) q.execute();
            assertEquals(1, c.size());
            Iterator it = c.iterator();
            assertEquals("one", ((PrimitiveObject) it.next()).getTestString());
            tx.commit();

        } finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }
    }

    /**
     * test ordering for JPQL
     */
    @SuppressWarnings("rawtypes")
    @Test
    public void testOrderingJPQL() {
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction tx = pm.currentTransaction();

        try {
            tx.begin();
            Query q = pm
                    .newQuery(
                            "javax.jdo.query.JPQL",
                            "SELECT doc FROM  com.spidertracks.datanucleus."
                                    + "basic.model.PrimitiveObject as doc ORDER BY doc.testString ");
            Collection c = (Collection) q.execute();
            assertEquals(3, c.size());
            Iterator it = c.iterator();
            assertEquals("one", ((PrimitiveObject) it.next()).getTestString());
            assertEquals("three", ((PrimitiveObject) it.next()).getTestString());
            assertEquals("two", ((PrimitiveObject) it.next()).getTestString());
            tx.commit();

        } finally {
            if (tx.isActive()) {
                tx.rollback();
            }
            pm.close();
        }
    }
}
