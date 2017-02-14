/*
 * zgdbm.c - bindings for gdbm
 *
 * This file is part of zsh, the Z shell.
 *
 * Copyright (c) 2008 Clint Adams
 * All rights reserved.
 *
 * Modifications copyright (c) 2017 Sebastian Gniazdowski
 * All rights reserved.
 *
 * Permission is hereby granted, without written agreement and without
 * license or royalty fees, to use, copy, modify, and distribute this
 * software and to distribute modified versions of this software for any
 * purpose, provided that the above copyright notice and the following
 * two paragraphs appear in all copies of this software.
 *
 * In no event shall Clint Adams or the Zsh Development
 * Group be liable to any party for direct, indirect, special, incidental, or
 * consequential damages arising out of the use of this software and its
 * documentation, even if Peter Stephenson, Sven Wischnowsky and the Zsh
 * Development Group have been advised of the possibility of such damage.
 *
 * Clint Adams and the Zsh Development Group
 * specifically disclaim any warranties, including, but not limited to, the
 * implied warranties of merchantability and fitness for a particular purpose.
 * The software provided hereunder is on an "as is" basis, and Peter
 * Stephenson, Sven Wischnowsky and the Zsh Development Group have no
 * obligation to provide maintenance, support, updates, enhancements, or
 * modifications.
 *
 */

#include "zgdbm.mdh"
#include "zgdbm.pro"

#ifndef PM_UPTODATE
#define PM_UPTODATE     (1<<19) /* Parameter has up-to-date data (e.g. loaded from DB) */
#endif

static Param createhash( char *name, int flags );

/*
 * Make sure we have all the bits I'm using for memory mapping, otherwise
 * I don't know what I'm doing.
 */
#if defined(HAVE_GDBM_H) && defined(HAVE_GDBM_OPEN)

#include <gdbm.h>

static char *backtype = "db/gdbm";

/*
 * Longer GSU structure, to carry GDBM_FILE of owning
 * database. Every parameter (hash value) receives GSU
 * pointer and thus also receives GDBM_FILE - this way
 * parameters can access proper database.
 *
 * Main HashTable parameter has the same instance of
 * the custom GSU struct in u.hash->tmpdata field.
 * When database is closed, `dbf` field is set to NULL
 * and hash values know to not access database when
 * being unset (total purge at zuntie).
 *
 * When database closing is ended, custom GSU struct
 * is freed. Only new ztie creates new custom GSU
 * struct instance.
 */

struct gsu_scalar_ext {
    struct gsu_scalar std;
    GDBM_FILE dbf;
};

/* Source structure - will be copied to allocated one,
 * with `dbf` filled. `dbf` allocation <-> gsu allocation. */
static const struct gsu_scalar_ext gdbm_gsu_ext =
{ { gdbmgetfn, gdbmsetfn, gdbmunsetfn }, 0 };

/**/
static const struct gsu_hash gdbm_hash_gsu =
{ hashgetfn, gdbmhashsetfn, gdbmhashunsetfn };

static struct builtin bintab[] = {
    BUILTIN("ztie", 0, bin_ztie, 1, -1, 0, "d:f:r", NULL),
    BUILTIN("zuntie", 0, bin_zuntie, 1, -1, 0, "u", NULL),
};

/**/
static int
bin_ztie(char *nam, char **args, Options ops, UNUSED(int func))
{
    char *resource_name, *pmname;
    GDBM_FILE dbf = NULL;
    int read_write = GDBM_SYNC, pmflags = PM_REMOVABLE;
    Param tied_param;

    if(!OPT_ISSET(ops,'d')) {
        zwarnnam(nam, "you must pass `-d %s'", backtype);
	return 1;
    }
    if(!OPT_ISSET(ops,'f')) {
        zwarnnam(nam, "you must pass `-f' with a filename", NULL);
	return 1;
    }
    if (OPT_ISSET(ops,'r')) {
	read_write |= GDBM_READER;
	pmflags |= PM_READONLY;
    } else {
	read_write |= GDBM_WRCREAT;
    }

    /* Here should be a lookup of the backend type against
     * a registry, if generam DB mechanism is to be added */
    if (strcmp(OPT_ARG(ops, 'd'), backtype) != 0) {
        zwarnnam(nam, "unsupported backend type `%s'", OPT_ARG(ops, 'd'));
	return 1;
    }

    resource_name = OPT_ARG(ops, 'f');
    pmname = *args;

    if ((tied_param = (Param)paramtab->getnode(paramtab, pmname)) &&
	!(tied_param->node.flags & PM_UNSET)) {
	/*
	 * Unset any existing parameter.  Note there's no implicit
	 * "local" here, but if the existing parameter is local
	 * then new parameter will be also local without following
         * unset.
	 *
	 * We need to do this before attempting to open the DB
	 * in case this variable is already tied to a DB.
	 *
	 * This can fail if the variable is readonly or restricted.
	 * We could call unsetparam() and check errflag instead
	 * of the return status.
	 */
	if (unsetparam_pm(tied_param, 0, 1))
	    return 1;
    }

    dbf = gdbm_open(resource_name, 0, read_write, 0666, 0);
    if(dbf)
	addmodulefd(gdbm_fdesc(dbf), FDT_MODULE);
    else {
	zwarnnam(nam, "error opening database file %s", resource_name);
	return 1;
    }

    if (!(tied_param = createhash(pmname, pmflags))) {
        zwarnnam(nam, "cannot create the requested parameter %s", pmname);
	fdtable[gdbm_fdesc(dbf)] = FDT_UNUSED;
	gdbm_close(dbf);
	return 1;
    }

    tied_param->gsu.h = &gdbm_hash_gsu;

    /* Allocate parameter sub-gsu, fill dbf field. 
     * dbf allocation is 1 to 1 accompanied by
     * gsu_scalar_ext allocation. */

    struct gsu_scalar_ext *dbf_carrier = (struct gsu_scalar_ext *) zalloc(sizeof(struct gsu_scalar_ext));
    dbf_carrier->std = gdbm_gsu_ext.std;
    dbf_carrier->dbf = dbf;
    tied_param->u.hash->tmpdata = (void *)dbf_carrier;

    return 0;
}

/**/
static int
bin_zuntie(char *nam, char **args, Options ops, UNUSED(int func))
{
    Param pm;
    char *pmname;
    int ret = 0;

    for (pmname = *args; *args++; pmname = *args) {
	pm = (Param) paramtab->getnode(paramtab, pmname);
	if(!pm) {
	    zwarnnam(nam, "cannot untie %s", pmname);
	    ret = 1;
	    continue;
	}
	if (pm->gsu.h != &gdbm_hash_gsu) {
	    zwarnnam(nam, "not a tied gdbm hash: %s", pmname);
	    ret = 1;
	    continue;
	}

	queue_signals();
	if (OPT_ISSET(ops,'u'))
	    gdbmuntie(pm);	/* clear read-only-ness */
	if (unsetparam_pm(pm, 0, 1)) {
	    /* assume already reported */
	    ret = 1;
	}
	unqueue_signals();
    }

    return ret;
}

/*
 * The param is actual param in hash – always, because
 * getgdbmnode creates every new key seen. However, it
 * might be not PM_UPTODATE - which means that database
 * wasn't yet queried.
 *
 * It will be left in this state if database doesn't
 * contain such key. That might be a drawback, maybe
 * setting to empty value has sense, as no other writer
 * can exist. This would remove subtle hcalloc(1) leak.
 */

/**/
static char *
gdbmgetfn(Param pm)
{
    datum key, content;
    int ret;
    GDBM_FILE dbf;

    /* Key already retrieved? There is no sense of asking the
     * database again, because:
     * - there can be only multiple readers
     * - so, no writer + reader use is allowed
     *
     * Thus:
     * - if we are writers, we for sure have newest copy of data
     * - if we are readers, we for sure have newest copy of data
     */
    if ( pm->node.flags & PM_UPTODATE ) {
        return pm->u.str ? pm->u.str : (char *) hcalloc(1);
    }

    key.dptr = pm->node.nam;
    key.dsize = strlen(key.dptr);

    dbf = ((struct gsu_scalar_ext *)pm->gsu.s)->dbf;

    if((ret = gdbm_exists(dbf, key))) {
        /* We have data – store it, return it */
        pm->node.flags |= PM_UPTODATE;

        content = gdbm_fetch(dbf, key);

        /* Ensure there's no leak */
        if (pm->u.str) {
            zsfree(pm->u.str);
        }

        pm->u.str = ztrduppfx( content.dptr, content.dsize );

        /* Can return pointer, correctly saved inside hash */
        return pm->u.str;
    }

    /* Can this be "" ? */
    return (char *) hcalloc(1);
}

/**/
static void
gdbmsetfn(Param pm, char *val)
{
    datum key, content;
    GDBM_FILE dbf;

    /* Set is done on parameter and on database.
     * See the allowed workers / readers comment
     * at gdbmgetfn() */

    /* Parameter */
    if (pm->u.str) {
        zsfree(pm->u.str);
        pm->u.str = NULL;
        pm->node.flags &= ~(PM_UPTODATE);
    }

    if (val) {
        pm->u.str = ztrdup(val);
        pm->node.flags |= PM_UPTODATE;
    }

    /* Database */
    key.dptr = pm->node.nam;
    key.dsize = strlen(key.dptr);
    dbf = ((struct gsu_scalar_ext *)pm->gsu.s)->dbf;

    if (val) {
        if (dbf) {
            content.dptr = val;
            content.dsize = strlen(content.dptr);
            (void)gdbm_store(dbf, key, content, GDBM_REPLACE);
        }
    } else {
        if (dbf) {
            (void)gdbm_delete(dbf, key);
        }
    }
}

/**/
static void
gdbmunsetfn(Param pm, UNUSED(int um))
{
    /* Set with NULL */
    gdbmsetfn(pm, NULL);
}

/**/
static HashNode
getgdbmnode(HashTable ht, const char *name)
{
    HashNode hn = gethashnode2( ht, name );
    Param val_pm = (Param) hn;

    /* Entry for key doesn't exist? Create it now,
     * it will be interfacing between the database
     * and Zsh - through special gdbm_gsu. So, any
     * seen key results in new interfacing parameter.
     *
     * Previous code was returning heap arena Param
     * that wasn't actually added to the hash. It was
     * plainly name / database-key holder. Here we
     * add the Param to its hash, it is not PM_UPTODATE.
     * It will be loaded from database *and filled*
     * or left in that state if the database doesn't
     * contain it.
     *
     * No heap arena memory is used, memory usage is
     * now limited - by number of distinct keys seen,
     * not by number of key *uses*.
     * */

    if ( ! val_pm ) {
        val_pm = (Param) zshcalloc( sizeof (*val_pm) );
        val_pm->node.flags = PM_SCALAR | PM_HASHELEM; /* no PM_UPTODATE */
        val_pm->gsu.s = (GsuScalar) ht->tmpdata;
        ht->addnode( ht, ztrdup( name ), val_pm ); // sets pm->node.nam
    }

    return (HashNode) val_pm;
}

/**/
static void
scangdbmkeys(HashTable ht, ScanFunc func, int flags)
{
    Param pm = NULL;
    datum key, content;
    GDBM_FILE dbf = ((struct gsu_scalar_ext *)ht->tmpdata)->dbf;

    /* Iterate keys adding them to hash, so
     * we have Param to use in `func` */
    key = gdbm_firstkey(dbf);

    while(key.dptr) {
        /* This returns database-interfacing Param,
         * it will return u.str or first fetch data
         * if not PM_UPTODATE (newly created) */
        char *zkey = ztrduppfx(key.dptr, key.dsize);
        HashNode hn = getgdbmnode(ht, zkey);
        zsfree( zkey );

	func(hn, flags);

        /* Iterate - no problem as interfacing Param
         * will do at most only fetches, not stores */
        key = gdbm_nextkey(dbf, key);
    }

}

/*
 * Replace database with new hash
 */

/**/
static void
gdbmhashsetfn(Param pm, HashTable ht)
{
    int i;
    HashNode hn;
    GDBM_FILE dbf;
    datum key, content;

    if (!pm->u.hash || pm->u.hash == ht)
	return;

    if (!(dbf = ((struct gsu_scalar_ext *)pm->u.hash->tmpdata)->dbf))
	return;

    key = gdbm_firstkey(dbf);
    while (key.dptr) {
	queue_signals();
	(void)gdbm_delete(dbf, key);
	free(key.dptr);
	unqueue_signals();
	key = gdbm_firstkey(dbf);
    }

    /* just deleted everything, clean up */
    (void)gdbm_reorganize(dbf);

    if (!ht)
	return;

     /* Put new strings into database, waiting
      * for their interfacing-Params to be created */

    for (i = 0; i < ht->hsize; i++)
	for (hn = ht->nodes[i]; hn; hn = hn->next) {
	    struct value v;

	    v.isarr = v.flags = v.start = 0;
	    v.end = -1;
	    v.arr = NULL;
	    v.pm = (Param) hn;

	    key.dptr = v.pm->node.nam;
	    key.dsize = strlen(key.dptr);

	    queue_signals();

	    content.dptr = getstrvalue(&v);
	    content.dsize = strlen(content.dptr);

	    (void)gdbm_store(dbf, key, content, GDBM_REPLACE);	

	    unqueue_signals();
	}
}

/**/
static void
gdbmuntie(Param pm)
{
    GDBM_FILE dbf = ((struct gsu_scalar_ext *)pm->u.hash->tmpdata)->dbf;
    HashTable ht = pm->u.hash;

    if (dbf) { /* paranoia */
	fdtable[gdbm_fdesc(dbf)] = FDT_UNUSED;
        gdbm_close(dbf);

        /* Let hash fields know there's no backend */
        ((struct gsu_scalar_ext *)ht->tmpdata)->dbf = NULL;
    }

    /* for completeness ... createspecialhash() should have an inverse */
    ht->getnode = ht->getnode2 = gethashnode2;
    ht->scantab = NULL;

    pm->node.flags &= ~(PM_SPECIAL|PM_READONLY);
    pm->gsu.h = &stdhash_gsu;
}

/**/
static void
gdbmhashunsetfn(Param pm, UNUSED(int exp))
{
    gdbmuntie(pm);

    /* Remember custom GSU structure assigned to
     * u.hash->tmpdata before hash gets deleted */
    void * gsu_ext = pm->u.hash->tmpdata;

    /* Uses normal unsetter. Will delete all owned
     * parameters and also hashtable. */
    pm->gsu.h->setfn(pm, NULL);

    /* Don't need custom GSU structure with its
     * GDBM_FILE pointer anymore */
    zfree( gsu_ext, sizeof(struct gsu_scalar_ext));

    pm->node.flags |= PM_UNSET;
}

#else
# error no gdbm
#endif /* have gdbm */

static struct features module_features = {
    bintab, sizeof(bintab)/sizeof(*bintab),
    NULL, 0,
    NULL, 0,
    NULL, 0,
    0
};

/**/
int
setup_(UNUSED(Module m))
{
    return 0;
}

/**/
int
features_(Module m, char ***features)
{
    *features = featuresarray(m, &module_features);
    return 0;
}

/**/
int
enables_(Module m, int **enables)
{
    return handlefeatures(m, &module_features, enables);
}

/**/
int
boot_(UNUSED(Module m))
{
    return 0;
}

/**/
int
cleanup_(Module m)
{
    return setfeatureenables(m, &module_features, NULL);
}

/**/
int
finish_(UNUSED(Module m))
{
    return 0;
}

static Param createhash( char *name, int flags ) {
    Param pm;
    HashTable ht;

    pm = createparam(name, PM_SPECIAL | PM_HASHED);
    if (!pm) {
        return NULL;
    }

    if (pm->old)
	pm->level = locallevel;

    /* This creates standard hash. */
    ht = pm->u.hash = newparamtable(32, name);
    if (!pm->u.hash) {
        paramtab->removenode(paramtab, name);
        paramtab->freenode(&pm->node);
        zwarnnam(name, "Out of memory when allocating hash");
    }

    /* These provide special features */
    ht->getnode = ht->getnode2 = getgdbmnode;
    ht->scantab = scangdbmkeys;

    return pm;
}
