package simpledb.index;

import java.io.*;
import java.util.*;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.execution.IndexPredicate;
import simpledb.execution.Predicate.Op;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * BTreeFile is an implementation of a DbFile that stores a B+ tree.
 * Specifically, it stores a pointer to a root page,
 * a set of internal pages, and a set of leaf pages, which contain a collection of tuples
 * in sorted order. BTreeFile works closely with BTreeLeafPage, BTreeInternalPage,
 * and BTreeRootPtrPage. The format of these pages is described in their constructors.
 *
 * @author Becca Taft
 * @see BTreeLeafPage#BTreeLeafPage
 * @see BTreeInternalPage#BTreeInternalPage
 * @see BTreeHeaderPage#BTreeHeaderPage
 * @see BTreeRootPtrPage#BTreeRootPtrPage
 */
public class BTreeFile implements DbFile {

    private final File f;
    private final TupleDesc td;
    private final int tableid;
    private final int keyField;

    /**
     * Constructs a B+ tree file backed by the specified file.
     *
     * @param f   - the file that stores the on-disk backing store for this B+ tree
     *            file.
     * @param key - the field which index is keyed on
     * @param td  - the tuple descriptor of tuples in the file
     */
    public BTreeFile(File f, int key, TupleDesc td) {
        this.f = f;
        this.tableid = f.getAbsoluteFile().hashCode();
        this.keyField = key;
        this.td = td;
    }

    /**
     * Returns the File backing this BTreeFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this BTreeFile. Implementation note:
     * you will need to generate this tableid somewhere and ensure that each
     * BTreeFile has a "unique id," and that you always return the same value for
     * a particular BTreeFile. We suggest hashing the absolute file name of the
     * file underlying the BTreeFile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this BTreeFile.
     */
    public int getId() {
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * Read a page from the file on disk. This should not be called directly
     * but should be called from the BufferPool via getPage()
     *
     * @param pid - the id of the page to read from disk
     * @return the page constructed from the contents on disk
     */
    public Page readPage(PageId pid) {
        BTreePageId id = (BTreePageId) pid;

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f))) {
            if (id.pgcateg() == BTreePageId.ROOT_PTR) {
                byte[] pageBuf = new byte[BTreeRootPtrPage.getPageSize()];
                int retval = bis.read(pageBuf, 0, BTreeRootPtrPage.getPageSize());
                if (retval == -1) {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (retval < BTreeRootPtrPage.getPageSize()) {
                    throw new IllegalArgumentException("Unable to read "
                            + BTreeRootPtrPage.getPageSize() + " bytes from BTreeFile");
                }
                Debug.log(1, "BTreeFile.readPage: read page %d", id.getPageNumber());
                return new BTreeRootPtrPage(id, pageBuf);
            } else {
                byte[] pageBuf = new byte[BufferPool.getPageSize()];
                if (bis.skip(BTreeRootPtrPage.getPageSize() + (long) (id.getPageNumber() - 1) * BufferPool.getPageSize()) !=
                        BTreeRootPtrPage.getPageSize() + (long) (id.getPageNumber() - 1) * BufferPool.getPageSize()) {
                    throw new IllegalArgumentException(
                            "Unable to seek to correct place in BTreeFile");
                }
                int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
                if (retval == -1) {
                    throw new IllegalArgumentException("Read past end of table");
                }
                if (retval < BufferPool.getPageSize()) {
                    throw new IllegalArgumentException("Unable to read "
                            + BufferPool.getPageSize() + " bytes from BTreeFile");
                }
                Debug.log(1, "BTreeFile.readPage: read page %d", id.getPageNumber());
                if (id.pgcateg() == BTreePageId.INTERNAL) {
                    return new BTreeInternalPage(id, pageBuf, keyField);
                } else if (id.pgcateg() == BTreePageId.LEAF) {
                    return new BTreeLeafPage(id, pageBuf, keyField);
                } else { // id.pgcateg() == BTreePageId.HEADER
                    return new BTreeHeaderPage(id, pageBuf);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Close the file on success or error
        // Ignore failures closing the file
    }

    /**
     * Write a page to disk.  This should not be called directly but should
     * be called from the BufferPool when pages are flushed to disk
     *
     * @param page - the page to write to disk
     */
    public void writePage(Page page) throws IOException {
        BTreePageId id = (BTreePageId) page.getId();

        byte[] data = page.getPageData();
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        if (id.pgcateg() == BTreePageId.ROOT_PTR) {
            rf.write(data);
            rf.close();
        } else {
            rf.seek(BTreeRootPtrPage.getPageSize() + (long) (page.getId().getPageNumber() - 1) * BufferPool.getPageSize());
            rf.write(data);
            rf.close();
        }
    }

    /**
     * Returns the number of pages in this BTreeFile.
     */
    public int numPages() {
        // we only ever write full pages
        return (int) ((f.length() - BTreeRootPtrPage.getPageSize()) / BufferPool.getPageSize());
    }

    /**
     * Returns the index of the field that this B+ tree is keyed on
     */
    public int keyField() {
        return keyField;
    }

    /**
     * Recursive function which finds and locks the leaf page in the B+ tree corresponding to
     * the left-most page possibly containing the key field f. It locks all internal
     * nodes along the path to the leaf node with READ_ONLY permission, and locks the
     * leaf node with permission perm.
     * <p>
     * If f is null, it finds the left-most leaf page -- used for the iterator
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid        - the current page being searched
     * @param perm       - the permissions with which to lock the leaf page
     * @param f          - the field to search for
     * @return the left-most leaf page possibly containing the key field f
     */
    private BTreeLeafPage findLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, Permissions perm,
                                       Field f)
            throws DbException, TransactionAbortedException {
        // some code goes here
        if (pid.pgcateg() == BTreePageId.LEAF) {
            return (BTreeLeafPage) getPage(tid, dirtypages, pid, perm);
        }
        BTreeInternalPage page = (BTreeInternalPage) getPage(tid, dirtypages, pid, Permissions.READ_ONLY);
        Iterator<BTreeEntry> iterator = page.iterator();
        while (iterator.hasNext()) {
            BTreeEntry next = iterator.next();
            // 因为要找到最左节点，所以相等也一律递归左孩子，即 f <= next.key 或 next.key >= f
            if (f == null || next.getKey().compare(Op.GREATER_THAN_OR_EQ, f)) {
                return findLeafPage(tid, dirtypages, next.getLeftChild(), perm, f);
            }
            // 如果迭代到最后一个了，那么就返回右节点
            if (!iterator.hasNext()) {
                return findLeafPage(tid, dirtypages, next.getRightChild(), perm, f);
            }
        }
        return null;
    }

    /**
     * Convenience method to find a leaf page when there is no dirtypages HashMap.
     * Used by the BTreeFile iterator.
     *
     * @param tid - the transaction id
     * @param pid - the current page being searched
     * @param f   - the field to search for
     * @return the left-most leaf page possibly containing the key field f
     * @see #findLeafPage(TransactionId, Map, BTreePageId, Permissions, Field)
     */
    BTreeLeafPage findLeafPage(TransactionId tid, BTreePageId pid,
                               Field f)
            throws DbException, TransactionAbortedException {
        return findLeafPage(tid, new HashMap<>(), pid, Permissions.READ_ONLY, f);
    }

    /**
     * Split a leaf page to make room for new tuples and recursively split the parent node
     * as needed to accommodate a new entry. The new entry should have a key matching the key field
     * of the first tuple in the right-hand page (the key is "copied up"), and child pointers
     * pointing to the two leaf pages resulting from the split.  Update sibling pointers and parent
     * pointers as needed.
     * <p>
     * Return the leaf page into which a new tuple with key field "field" should be inserted.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the leaf page to split
     * @param field      - the key field of the tuple to be inserted after the split is complete. Necessary to know
     *                   which of the two pages to return.
     * @return the leaf page into which the new tuple should be inserted
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #getParentWithEmptySlots(TransactionId, Map, BTreePageId, Field)
     */
    public BTreeLeafPage splitLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage page, Field field)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //
        // Split the leaf page by adding a new page on the right of the existing
        // page and moving half of the tuples to the new page.  Copy the middle key up
        // into the parent page, and recursively split the parent as needed to accommodate
        // the new entry.  getParentWithEmtpySlots() will be useful here.  Don't forget to update
        // the sibling pointers of all the affected leaf pages.  Return the page into which a
        // tuple with the given key field should be inserted.
        BTreeLeafPage newPage = (BTreeLeafPage) getEmptyPage(tid, dirtypages, BTreePageId.LEAF);
        // 将 page 中的一半移到新页去，更正式的说，如果有 N 个元组，那么 [0, N/2) 作为此页的元组，[N/2，N) 作为新页的元组，N/2 下标处的元组将被向上移动
        int n = page.getNumTuples(), i = 0;
        Iterator<Tuple> iterator = page.iterator();
        while (iterator.hasNext()) {
            Tuple next = iterator.next();
            if (i++ >= n / 2) {
                // 删除操作与迭代器不会冲突
                page.deleteTuple(next);
                newPage.insertTuple(next);
            }
        }
        BTreePageId oldRight = page.getRightSiblingId();
        if (oldRight != null) {
            newPage.setRightSiblingId(oldRight);
            ((BTreeLeafPage) getPage(tid, dirtypages, oldRight, Permissions.READ_WRITE)).setLeftSiblingId(newPage.getId());
        }
        page.setRightSiblingId(newPage.getId());
        newPage.setLeftSiblingId(page.getId());

        Field midField = newPage.getTuple(0).getField(this.keyField);
        BTreeInternalPage parent = getParentWithEmptySlots(tid, dirtypages, page.getParentId(), midField);
        parent.insertEntry(new BTreeEntry(midField, page.getId(), newPage.getId()));

        updateParentPointer(tid, dirtypages, parent.getId(), page.getId());
        updateParentPointer(tid, dirtypages, parent.getId(), newPage.getId());


        page.markDirty(true, tid);
        newPage.markDirty(true, tid);
        parent.markDirty(true, tid);
        dirtypages.put(page.getId(), page);
        dirtypages.put(newPage.getId(), newPage);
        dirtypages.put(parent.getId(), parent);

        // 实际要插入哪个页面？如果页面只能容纳下一个元组，那么左页面是空的，否则需要与中间节点比较
        if (n == 1)
            return page;

        return midField.compare(Op.GREATER_THAN_OR_EQ, field) ? page : newPage;
    }

    /**
     * Split an internal page to make room for new entries and recursively split its parent page
     * as needed to accommodate a new entry. The new entry for the parent should have a key matching
     * the middle key in the original internal page being split (this key is "pushed up" to the parent).
     * The child pointers of the new parent entry should point to the two internal pages resulting
     * from the split. Update parent pointers as needed.
     * <p>
     * Return the internal page into which an entry with key field "field" should be inserted
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the internal page to split
     * @param field      - the key field of the entry to be inserted after the split is complete. Necessary to know
     *                   which of the two pages to return.
     * @return the internal page into which the new entry should be inserted
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #getParentWithEmptySlots(TransactionId, Map, BTreePageId, Field)
     * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
     */
    public BTreeInternalPage splitInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
                                               BTreeInternalPage page, Field field)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //
        // Split the internal page by adding a new page on the right of the existing
        // page and moving half of the entries to the new page.  Push the middle key up
        // into the parent page, and recursively split the parent as needed to accommodate
        // the new entry.  getParentWithEmtpySlots() will be useful here.  Don't forget to update
        // the parent pointers of all the children moving to the new page.  updateParentPointers()
        // will be useful here.  Return the page into which an entry with the given key field
        // should be inserted.
        BTreeInternalPage newPage = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);
        // 与 Leaf 分裂有一点小差距，新页不会包含中间元组
        // 如果有 N 个元组，那么 [0, N/2) 作为此页的元组，(N/2，N) 作为新页的元组，N/2 下标处的元组将被向上移动
        int n = page.getNumEntries(), i = 0;
        Iterator<BTreeEntry> iterator = page.iterator();
        BTreeEntry midEntry = null;
        while (iterator.hasNext()) {
            var next = iterator.next();
            if (i >= n / 2) {
                // delete left 和 delete right 有什么区别呢？删除 left 有一定概率无法通过测试
                page.deleteKeyAndRightChild(next);
                if (i == n / 2)
                    midEntry = next;
                else
                    newPage.insertEntry(next);
            }
            i++;
        }
        assert midEntry != null;
        midEntry.setLeftChild(page.getId());
        midEntry.setRightChild(newPage.getId());

        Field midField = midEntry.getKey();
        BTreeInternalPage parent = getParentWithEmptySlots(tid, dirtypages, page.getParentId(), midField);
        parent.insertEntry(midEntry);
//        updateParentPointers(tid, dirtypages, page);
        updateParentPointers(tid, dirtypages, newPage);
//        updateParentPointers(tid, dirtypages, parent);
        page.markDirty(true, tid);
        newPage.markDirty(true, tid);
        parent.markDirty(true, tid);
        dirtypages.put(page.getId(), page);
        dirtypages.put(newPage.getId(), newPage);
        dirtypages.put(parent.getId(), parent);

        return midField.compare(Op.GREATER_THAN_OR_EQ, field) ? page : newPage;
    }

    /**
     * Method to encapsulate the process of getting a parent page ready to accept new entries.
     * This may mean creating a page to become the new root of the tree, splitting the existing
     * parent page if there are no empty slots, or simply locking and returning the existing parent page.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param parentId   - the id of the parent. May be an internal page or the RootPtr page
     * @param field      - the key of the entry which will be inserted. Needed in case the parent must be split
     *                   to accommodate the new entry
     * @return the parent page, guaranteed to have at least one empty slot
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #splitInternalPage(TransactionId, Map, BTreeInternalPage, Field)
     */
    private BTreeInternalPage getParentWithEmptySlots(TransactionId tid, Map<PageId, Page> dirtypages,
                                                      BTreePageId parentId, Field field) throws DbException, IOException, TransactionAbortedException {

        BTreeInternalPage parent = null;

        // create a parent node if necessary
        // this will be the new root of the tree
        if (parentId.pgcateg() == BTreePageId.ROOT_PTR) {
            parent = (BTreeInternalPage) getEmptyPage(tid, dirtypages, BTreePageId.INTERNAL);

            // update the root pointer
            BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages,
                    BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
            BTreePageId prevRootId = rootPtr.getRootId(); //save prev id before overwriting.
            rootPtr.setRootId(parent.getId());

            // update the previous root to now point to this new root.
            BTreePage prevRootPage = (BTreePage) getPage(tid, dirtypages, prevRootId, Permissions.READ_WRITE);
            prevRootPage.setParentId(parent.getId());
        } else {
            // lock the parent page
            parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId,
                    Permissions.READ_WRITE);
        }

        // split the parent if needed
        if (parent.getNumEmptySlots() == 0) {
            parent = splitInternalPage(tid, dirtypages, parent, field);
        }

        return parent;

    }

    /**
     * Helper function to update the parent pointer of a node.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid        - id of the parent node
     * @param child      - id of the child node to be updated with the parent pointer
     * @throws DbException
     * @throws TransactionAbortedException
     */
    private void updateParentPointer(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, BTreePageId child)
            throws DbException, TransactionAbortedException {

        BTreePage p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_ONLY);

        if (!p.getParentId().equals(pid)) {
            p = (BTreePage) getPage(tid, dirtypages, child, Permissions.READ_WRITE);
            p.setParentId(pid);
        }
    }

    /**
     * Update the parent pointer of every child of the given page so that it correctly points to
     * the parent
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the parent page
     * @throws DbException
     * @throws TransactionAbortedException
     * @see #updateParentPointer(TransactionId, Map, BTreePageId, BTreePageId)
     */
    private void updateParentPointers(TransactionId tid, Map<PageId, Page> dirtypages, BTreeInternalPage page)
            throws DbException, TransactionAbortedException {
        Iterator<BTreeEntry> it = page.iterator();
        BTreePageId pid = page.getId();
        BTreeEntry e = null;
        while (it.hasNext()) {
            e = it.next();
            updateParentPointer(tid, dirtypages, pid, e.getLeftChild());
        }
        if (e != null) {
            updateParentPointer(tid, dirtypages, pid, e.getRightChild());
        }
    }

    /**
     * Method to encapsulate the process of locking/fetching a page.  First the method checks the local
     * cache ("dirtypages"), and if it can't find the requested page there, it fetches it from the buffer pool.
     * It also adds pages to the dirtypages cache if they are fetched with read-write permission, since
     * presumably they will soon be dirtied by this transaction.
     * <p>
     * This method is needed to ensure that page updates are not lost if the same pages are
     * accessed multiple times.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pid        - the id of the requested page
     * @param perm       - the requested permissions on the page
     * @return the requested page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    Page getPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePageId pid, Permissions perm)
            throws DbException, TransactionAbortedException {
        if (dirtypages.containsKey(pid)) {
            return dirtypages.get(pid);
        } else {
            Page p = Database.getBufferPool().getPage(tid, pid, perm);
            if (perm == Permissions.READ_WRITE) {
                dirtypages.put(pid, p);
            }
            return p;
        }
    }

    /**
     * Insert a tuple into this BTreeFile, keeping the tuples in sorted order.
     * May cause pages to split if the page where tuple t belongs is full.
     *
     * @param tid - the transaction id
     * @param t   - the tuple to insert
     * @return a list of all pages that were dirtied by this operation. Could include
     * many pages since parent pointers will need to be updated when an internal node splits.
     * @see #splitLeafPage(TransactionId, Map, BTreeLeafPage, Field)
     */
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        Map<PageId, Page> dirtypages = new HashMap<>();

        // 获取根指针页面的读锁并使用它来定位根页面
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId rootId = rootPtr.getRootId();

        if (rootId == null) { // root 刚刚创建，所以设置 root 指针指向它
            rootId = new BTreePageId(tableid, numPages(), BTreePageId.LEAF);
            rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);
            rootPtr.setRootId(rootId);
        }

        // 找到key字段对应的最左边的leaf page并锁定，如果没有slots可用则拆分leaf page
        int v = (int) t.getField(keyField).getObject();

        BTreeLeafPage leafPage = findLeafPage(tid, dirtypages, rootId, Permissions.READ_WRITE, t.getField(keyField));


        assert leafPage != null;
        if (leafPage.getNumEmptySlots() == 0) {
            leafPage = splitLeafPage(tid, dirtypages, leafPage, t.getField(keyField));
        }

        // insert the tuple into the leaf page
        leafPage.insertTuple(t);
        if (v >= 502) {
//            System.out.println("v = " + (v) + " currSize" + (leafPage.getNumTuples()));
        }
        return new ArrayList<>(dirtypages.values());
    }

    /**
     * Handle the case when a B+ tree page becomes less than half full due to deletions.
     * If one of its siblings has extra tuples/entries, redistribute those tuples/entries.
     * Otherwise merge with one of the siblings. Update pointers as needed.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the page which is less than half full
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #handleMinOccupancyLeafPage(TransactionId, Map, BTreeLeafPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
     * @see #handleMinOccupancyInternalPage(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeEntry, BTreeEntry)
     */
    private void handleMinOccupancyPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreePage page)
            throws DbException, IOException, TransactionAbortedException {
        BTreePageId parentId = page.getParentId();
        BTreeEntry leftEntry = null;
        BTreeEntry rightEntry = null;
        BTreeInternalPage parent = null;

        // find the left and right siblings through the parent so we make sure they have
        // the same parent as the page. Find the entries in the parent corresponding to
        // the page and siblings
        if (parentId.pgcateg() != BTreePageId.ROOT_PTR) {
            parent = (BTreeInternalPage) getPage(tid, dirtypages, parentId, Permissions.READ_WRITE);
            Iterator<BTreeEntry> ite = parent.iterator();
            while (ite.hasNext()) {
                BTreeEntry e = ite.next();
                if (e.getLeftChild().equals(page.getId())) {
                    rightEntry = e;
                    break;
                } else if (e.getRightChild().equals(page.getId())) {
                    leftEntry = e;
                }
            }
        }

        if (page.getId().pgcateg() == BTreePageId.LEAF) {
            handleMinOccupancyLeafPage(tid, dirtypages, (BTreeLeafPage) page, parent, leftEntry, rightEntry);
        } else { // BTreePageId.INTERNAL
            handleMinOccupancyInternalPage(tid, dirtypages, (BTreeInternalPage) page, parent, leftEntry, rightEntry);
        }
    }

    /**
     * Handle the case when a leaf page becomes less than half full due to deletions.
     * If one of its siblings has extra tuples, redistribute those tuples.
     * Otherwise merge with one of the siblings. Update pointers as needed.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the leaf page which is less than half full
     * @param parent     - the parent of the leaf page
     * @param leftEntry  - the entry in the parent pointing to the given page and its left-sibling
     * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #mergeLeafPages(TransactionId, Map, BTreeLeafPage, BTreeLeafPage, BTreeInternalPage, BTreeEntry)
     * @see #stealFromLeafPage(BTreeLeafPage, BTreeLeafPage, BTreeInternalPage, BTreeEntry, boolean)
     */
    private void handleMinOccupancyLeafPage(TransactionId tid, Map<PageId, Page> dirtypages, BTreeLeafPage page,
                                            BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
            throws DbException, IOException, TransactionAbortedException {
        BTreePageId leftSiblingId = null;
        BTreePageId rightSiblingId = null;
        if (leftEntry != null) leftSiblingId = leftEntry.getLeftChild();
        if (rightEntry != null) rightSiblingId = rightEntry.getRightChild();
        // 5 - 5 / 2 = 3
        int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples() / 2; // ceiling
        if (leftSiblingId != null) {
            BTreeLeafPage leftSibling = (BTreeLeafPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
            // if the left sibling is at minimum occupancy, merge with it. Otherwise
            // steal some tuples from it
            if (leftSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeLeafPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
            } else {
                stealFromLeafPage(page, leftSibling, parent, leftEntry, false);
            }
        } else if (rightSiblingId != null) {
            BTreeLeafPage rightSibling = (BTreeLeafPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
            // if the right sibling is at minimum occupancy, merge with it. Otherwise
            // steal some tuples from it
            if (rightSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeLeafPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
            } else {
                stealFromLeafPage(page, rightSibling, parent, rightEntry, true);
            }
        }
    }

    /**
     * Steal tuples from a sibling and copy them to the given page so that both pages are at least
     * half full.  Update the parent's entry so that the key matches the key field of the first
     * tuple in the right-hand page.
     *
     * @param page           - the leaf page which is less than half full
     * @param sibling        - the sibling which has tuples to spare
     * @param parent         - the parent of the two leaf pages
     * @param entry          - the entry in the parent pointing to the two leaf pages
     * @param isRightSibling - whether the sibling is a right-sibling
     * @throws DbException
     */
    public void stealFromLeafPage(BTreeLeafPage page, BTreeLeafPage sibling,
                                  BTreeInternalPage parent, BTreeEntry entry, boolean isRightSibling) throws DbException {
        // some code goes here
        //
        // Move some of the tuples from the sibling to the page so
        // that the tuples are evenly distributed. Be sure to update
        // the corresponding parent entry.
//        int stealCount = (page.getNumTuples() + sibling.getNumTuples()) / 2 - page.getNumTuples();
////        if (sibling.getNumTuples() <= stealCount) {
////            return;
////        }
////        Iterator<Tuple> iterator = null;
////        if (!isRightSibling) {
////            iterator = sibling.reverseIterator();
////        } else {
////            iterator = sibling.iterator();
////        }
////        while (stealCount-- > 0) {
////            Tuple next = iterator.next();
////            sibling.deleteTuple(next);
////            page.insertTuple(next);
////        }
////        // 以左兄弟的最后一个元组或者右兄弟的第一个元组作为中间值
////        // 由于父节点实际上没有改变，指针不需要变动，直接更新 entry 即可
////        entry.setKey(iterator.next().getField(keyField));
////        parent.updateEntry(entry);

        int curTuples = page.getNumTuples();
        int siblingTuples = sibling.getNumTuples();
        int targetTuples = curTuples + siblingTuples >> 1;
        if (isRightSibling) {
            //从右兄弟steal
            Iterator<Tuple> it = sibling.iterator();
            while (it.hasNext() && curTuples < targetTuples) {
                Tuple t = it.next();
                //这里要先delete后insert，因为insert过程中，会将记录的pid改变，删除时记录的pid和当前页面的pid不匹配时会报错
                sibling.deleteTuple(t);
                page.insertTuple(t);
                curTuples++;
            }
            Tuple mid = sibling.iterator().next();
            entry.setKey(mid.getField(parent.keyField));
            parent.updateEntry(entry);
            return;
        }
        //从左兄弟steal
        Iterator<Tuple> it = sibling.reverseIterator();
        while (curTuples < targetTuples) {
            Tuple t = it.next();
            sibling.deleteTuple(t);
            page.insertTuple(t);
            curTuples++;
        }
        Tuple mid = page.iterator().next();
        entry.setKey(mid.getField(parent.keyField));
        parent.updateEntry(entry);
    }

    /**
     * Handle the case when an internal page becomes less than half full due to deletions.
     * If one of its siblings has extra entries, redistribute those entries.
     * Otherwise merge with one of the siblings. Update pointers as needed.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param page       - the internal page which is less than half full
     * @param parent     - the parent of the internal page
     * @param leftEntry  - the entry in the parent pointing to the given page and its left-sibling
     * @param rightEntry - the entry in the parent pointing to the given page and its right-sibling
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #mergeInternalPages(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
     * @see #stealFromLeftInternalPage(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
     * @see #stealFromRightInternalPage(TransactionId, Map, BTreeInternalPage, BTreeInternalPage, BTreeInternalPage, BTreeEntry)
     */
    private void handleMinOccupancyInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
                                                BTreeInternalPage page, BTreeInternalPage parent, BTreeEntry leftEntry, BTreeEntry rightEntry)
            throws DbException, IOException, TransactionAbortedException {
        BTreePageId leftSiblingId = null;
        BTreePageId rightSiblingId = null;
        if (leftEntry != null) leftSiblingId = leftEntry.getLeftChild();
        if (rightEntry != null) rightSiblingId = rightEntry.getRightChild();

        int maxEmptySlots = page.getMaxEntries() - page.getMaxEntries() / 2; // ceiling
        if (leftSiblingId != null) {
            BTreeInternalPage leftSibling = (BTreeInternalPage) getPage(tid, dirtypages, leftSiblingId, Permissions.READ_WRITE);
            // if the left sibling is at minimum occupancy, merge with it. Otherwise
            // steal some entries from it
            if (leftSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeInternalPages(tid, dirtypages, leftSibling, page, parent, leftEntry);
            } else {
                stealFromLeftInternalPage(tid, dirtypages, page, leftSibling, parent, leftEntry);
            }
        } else if (rightSiblingId != null) {
            BTreeInternalPage rightSibling = (BTreeInternalPage) getPage(tid, dirtypages, rightSiblingId, Permissions.READ_WRITE);
            // if the right sibling is at minimum occupancy, merge with it. Otherwise
            // steal some entries from it
            if (rightSibling.getNumEmptySlots() >= maxEmptySlots) {
                mergeInternalPages(tid, dirtypages, page, rightSibling, parent, rightEntry);
            } else {
                stealFromRightInternalPage(tid, dirtypages, page, rightSibling, parent, rightEntry);
            }
        }
    }

    /**
     * Steal entries from the left sibling and copy them to the given page so that both pages are at least
     * half full. Keys can be thought of as rotating through the parent entry, so the original key in the
     * parent is "pulled down" to the right-hand page, and the last key in the left-hand page is "pushed up"
     * to the parent.  Update parent pointers as needed.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param page        - the internal page which is less than half full
     * @param sibling     - the left sibling which has entries to spare
     * @param parent      - the parent of the two internal pages
     * @param parentEntry - the entry in the parent pointing to the two internal pages
     * @throws DbException
     * @throws TransactionAbortedException
     * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
     */
    public void stealFromLeftInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
                                          BTreeInternalPage page, BTreeInternalPage sibling, BTreeInternalPage parent,
                                          BTreeEntry parentEntry) throws DbException, TransactionAbortedException {
        // some code goes here
        // Move some entries from the left sibling to the page so
        // that the entries are evenly distributed. Be sure to update
        // the corresponding parent entry. Be sure to update the parent
        // pointers of all children in the entries that were moved.
        int stealCount = (page.getNumEntries() + sibling.getNumEntries()) / 2 - page.getNumEntries();
        if (sibling.getNumEntries() <= stealCount) {
            return;
        }
        // 先窃取父节点的值，注意左右指针更新
        // 左指针为兄弟节点的边缘指针，右指针为自身最左侧的指针
        BTreeEntry entry = new BTreeEntry(
                parentEntry.getKey(),
                sibling.reverseIterator().next().getRightChild(),
                page.iterator().next().getLeftChild());
        page.insertEntry(entry);
        stealCount--;

        Iterator<BTreeEntry> iterator = sibling.reverseIterator();
        for (int i = 0; i < stealCount; i++) {
            var next = iterator.next();
            sibling.deleteKeyAndRightChild(next);
            page.insertEntry(next);
        }

        BTreeEntry mid = iterator.next();
        sibling.deleteKeyAndRightChild(mid);
        parentEntry.setKey(mid.getKey());
        parent.updateEntry(parentEntry);

        updateParentPointers(tid, dirtypages, parent);
        updateParentPointers(tid, dirtypages, page);
        updateParentPointers(tid, dirtypages, sibling);
    }

    /**
     * Steal entries from the right sibling and copy them to the given page so that both pages are at least
     * half full. Keys can be thought of as rotating through the parent entry, so the original key in the
     * parent is "pulled down" to the left-hand page, and the last key in the right-hand page is "pushed up"
     * to the parent.  Update parent pointers as needed.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param page        - the internal page which is less than half full
     * @param sibling     - the right sibling which has entries to spare
     * @param parent      - the parent of the two internal pages
     * @param parentEntry - the entry in the parent pointing to the two internal pages
     * @throws DbException
     * @throws TransactionAbortedException
     * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
     */
    public void stealFromRightInternalPage(TransactionId tid, Map<PageId, Page> dirtypages,
                                           BTreeInternalPage page, BTreeInternalPage sibling, BTreeInternalPage parent,
                                           BTreeEntry parentEntry) throws DbException, TransactionAbortedException {
        // some code goes here
        // Move some entries from the right sibling to the page so
        // that the entries are evenly distributed. Be sure to update
        // the corresponding parent entry. Be sure to update the parent
        // pointers of all children in the entries that were moved.
        int stealCount = (page.getNumEntries() + sibling.getNumEntries()) / 2 - page.getNumEntries();
        if (sibling.getNumEntries() <= stealCount) {
            return;
        }
        // 先窃取父节点的值，注意左右指针更新
        // 左指针为自身最右侧指针，右指针为右兄弟的边缘指针
        BTreeEntry entry = new BTreeEntry(
                parentEntry.getKey(),
                page.reverseIterator().next().getRightChild(),
                sibling.iterator().next().getLeftChild());
        page.insertEntry(entry);
        stealCount--;

        Iterator<BTreeEntry> iterator = sibling.iterator();

        for (int i = 0; i < stealCount; i++) {
            var next = iterator.next();
            // 删除右兄弟的话，这里删除应该是删除左孩子
            sibling.deleteKeyAndLeftChild(next);
            page.insertEntry(next);
        }
        BTreeEntry mid = iterator.next();
        sibling.deleteKeyAndLeftChild(mid);
        parentEntry.setKey(mid.getKey());
        parent.updateEntry(parentEntry);

        updateParentPointers(tid, dirtypages, parent);
        updateParentPointers(tid, dirtypages, page);
        updateParentPointers(tid, dirtypages, sibling);
    }

    /**
     * Merge two leaf pages by moving all tuples from the right page to the left page.
     * Delete the corresponding key and right child pointer from the parent, and recursively
     * handle the case when the parent gets below minimum occupancy.
     * Update sibling pointers as needed, and make the right page available for reuse.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param leftPage    - the left leaf page
     * @param rightPage   - the right leaf page
     * @param parent      - the parent of the two pages
     * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #deleteParentEntry(TransactionId, Map, BTreePage, BTreeInternalPage, BTreeEntry)
     */
    public void mergeLeafPages(TransactionId tid, Map<PageId, Page> dirtypages,
                               BTreeLeafPage leftPage, BTreeLeafPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, IOException, TransactionAbortedException {

        // some code goes here
        //
        // Move all the tuples from the right page to the left page, update
        // the sibling pointers, and make the right page available for reuse.
        // Delete the entry in the parent corresponding to the two pages that are merging -
        // deleteParentEntry() will be useful here
        Iterator<Tuple> iterator = rightPage.iterator();
        while (iterator.hasNext()) {
            Tuple next = iterator.next();
            rightPage.deleteTuple(next);
            leftPage.insertTuple(next);
        }
        if (rightPage.getRightSiblingId() != null) {
            BTreeLeafPage page = (BTreeLeafPage) getPage(tid, dirtypages, rightPage.getRightSiblingId(), Permissions.READ_WRITE);
            if (page != null) {
                page.setLeftSiblingId(leftPage.getId());
            }
        }
        leftPage.setRightSiblingId(rightPage.getRightSiblingId());

        setEmptyPage(tid, dirtypages, rightPage.getId().getPageNumber());
        deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);
    }

    /**
     * Merge two internal pages by moving all entries from the right page to the left page
     * and "pulling down" the corresponding key from the parent entry.
     * Delete the corresponding key and right child pointer from the parent, and recursively
     * handle the case when the parent gets below minimum occupancy.
     * Update parent pointers as needed, and make the right page available for reuse.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param leftPage    - the left internal page
     * @param rightPage   - the right internal page
     * @param parent      - the parent of the two pages
     * @param parentEntry - the entry in the parent corresponding to the leftPage and rightPage
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #deleteParentEntry(TransactionId, Map, BTreePage, BTreeInternalPage, BTreeEntry)
     * @see #updateParentPointers(TransactionId, Map, BTreeInternalPage)
     */
    public void mergeInternalPages(TransactionId tid, Map<PageId, Page> dirtypages,
                                   BTreeInternalPage leftPage, BTreeInternalPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, IOException, TransactionAbortedException {

        // some code goes here
        //
        // Move all the entries from the right page to the left page, update
        // the parent pointers of the children in the entries that were moved,
        // and make the right page available for reuse
        // Delete the entry in the parent corresponding to the two pages that are merging -
        // deleteParentEntry() will be useful here
        // 先插入父节点
        BTreeEntry entry = new BTreeEntry(
                parentEntry.getKey(),
                leftPage.reverseIterator().next().getRightChild(),
                rightPage.iterator().next().getLeftChild());
        leftPage.insertEntry(entry);
        deleteParentEntry(tid, dirtypages, leftPage, parent, parentEntry);

        //开始将右边的entry转移到左边
        Iterator<BTreeEntry> it = rightPage.iterator();
        while (it.hasNext()) {
            BTreeEntry e = it.next();
            rightPage.deleteKeyAndLeftChild(e);
            leftPage.insertEntry(e);
        }

        updateParentPointers(tid, dirtypages, leftPage);
        setEmptyPage(tid, dirtypages, rightPage.pid.getPageNumber());
    }

    /**
     * Method to encapsulate the process of deleting an entry (specifically the key and right child)
     * from a parent node.  If the parent becomes empty (no keys remaining), that indicates that it
     * was the root node and should be replaced by its one remaining child.  Otherwise, if it gets
     * below minimum occupancy for non-root internal nodes, it should steal from one of its siblings or
     * merge with a sibling.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param leftPage    - the child remaining after the key and right child are deleted
     * @param parent      - the parent containing the entry to be deleted
     * @param parentEntry - the entry to be deleted
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #handleMinOccupancyPage(TransactionId, Map, BTreePage)
     */
    private void deleteParentEntry(TransactionId tid, Map<PageId, Page> dirtypages,
                                   BTreePage leftPage, BTreeInternalPage parent, BTreeEntry parentEntry)
            throws DbException, IOException, TransactionAbortedException {

        // delete the entry in the parent.  If
        // the parent is below minimum occupancy, get some tuples from its siblings
        // or merge with one of the siblings
        parent.deleteKeyAndRightChild(parentEntry);
        // 5 - 5 / 2 = 3
        int maxEmptySlots = parent.getMaxEntries() - parent.getMaxEntries() / 2; // ceiling
        if (parent.getNumEmptySlots() == parent.getMaxEntries()) {
            // This was the last entry in the parent.
            // In this case, the parent (root node) should be deleted, and the merged
            // page will become the new root
            BTreePageId rootPtrId = parent.getParentId();
            if (rootPtrId.pgcateg() != BTreePageId.ROOT_PTR) {
                throw new DbException("attempting to delete a non-root node");
            }
            BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, rootPtrId, Permissions.READ_WRITE);
            leftPage.setParentId(rootPtrId);
            rootPtr.setRootId(leftPage.getId());

            // release the parent page for reuse
            setEmptyPage(tid, dirtypages, parent.getId().getPageNumber());
        } else if (parent.getNumEmptySlots() > maxEmptySlots) {
            // <= 2
            handleMinOccupancyPage(tid, dirtypages, parent);
        }
    }

    /**
     * Delete a tuple from this BTreeFile.
     * May cause pages to merge or redistribute entries/tuples if the pages
     * become less than half full.
     *
     * @param tid - the transaction id
     * @param t   - the tuple to delete
     * @return a list of all pages that were dirtied by this operation. Could include
     * many pages since parent pointers will need to be updated when an internal node merges.
     * @see #handleMinOccupancyPage(TransactionId, Map, BTreePage)
     */
    public List<Page> deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        Map<PageId, Page> dirtypages = new HashMap<>();

        BTreePageId pageId = new BTreePageId(tableid, t.getRecordId().getPageId().getPageNumber(),
                BTreePageId.LEAF);
        BTreeLeafPage page = (BTreeLeafPage) getPage(tid, dirtypages, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);

        // if the page is below minimum occupancy, get some tuples from its siblings
        // or merge with one of the siblings
        int maxEmptySlots = page.getMaxTuples() - page.getMaxTuples() / 2; // ceiling
        if (page.getNumEmptySlots() > maxEmptySlots) {
            handleMinOccupancyPage(tid, dirtypages, page);
        }

        return new ArrayList<>(dirtypages.values());
    }

    /**
     * Get a read lock on the root pointer page. Create the root pointer page and root page
     * if necessary.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @return the root pointer page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    BTreeRootPtrPage getRootPtrPage(TransactionId tid, Map<PageId, Page> dirtypages) throws DbException, IOException, TransactionAbortedException {
        synchronized (this) {
            if (f.length() == 0) {
                // create the root pointer page and the root page
                BufferedOutputStream bw = new BufferedOutputStream(
                        new FileOutputStream(f, true));
                byte[] emptyRootPtrData = BTreeRootPtrPage.createEmptyPageData();
                byte[] emptyLeafData = BTreeLeafPage.createEmptyPageData();
                bw.write(emptyRootPtrData);
                bw.write(emptyLeafData);
                bw.close();
            }
        }

        // get a read lock on the root pointer page
        return (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_ONLY);
    }

    /**
     * Get the page number of the first empty page in this BTreeFile.
     * Creates a new page if none of the existing pages are empty.
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @return the page number of the first empty page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    public int getEmptyPageNo(TransactionId tid, Map<PageId, Page> dirtypages)
            throws DbException, IOException, TransactionAbortedException {
        // get a read lock on the root pointer page and use it to locate the first header page
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId headerId = rootPtr.getHeaderId();
        int emptyPageNo = 0;

        if (headerId != null) {
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
            int headerPageCount = 0;
            // try to find a header page with an empty slot
            while (headerPage != null && headerPage.getEmptySlot() == -1) {
                headerId = headerPage.getNextPageId();
                if (headerId != null) {
                    headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
                    headerPageCount++;
                } else {
                    headerPage = null;
                }
            }

            // if headerPage is not null, it must have an empty slot
            if (headerPage != null) {
                headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
                int emptySlot = headerPage.getEmptySlot();
                headerPage.markSlotUsed(emptySlot, true);
                emptyPageNo = headerPageCount * BTreeHeaderPage.getNumSlots() + emptySlot;
            }
        }

        // 此时如果 headerId 为 null，则要么没有标题页，要么没有空闲插槽
        if (headerId == null) {
            synchronized (this) {
                // create the new page
                BufferedOutputStream bw = new BufferedOutputStream(
                        new FileOutputStream(f, true));
                byte[] emptyData = BTreeInternalPage.createEmptyPageData();
                bw.write(emptyData);
                bw.close();
                emptyPageNo = numPages();
            }
        }

        return emptyPageNo;
    }

    /**
     * Method to encapsulate the process of creating a new page.  It reuses old pages if possible,
     * and creates a new page if none are available.  It wipes the page on disk and in the cache and
     * returns a clean copy locked with read-write permission
     *
     * @param tid        - the transaction id
     * @param dirtypages - the list of dirty pages which should be updated with all new dirty pages
     * @param pgcateg    - the BTreePageId category of the new page.  Either LEAF, INTERNAL, or HEADER
     * @return the new empty page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #getEmptyPageNo(TransactionId, Map)
     * @see #setEmptyPage(TransactionId, Map, int)
     */
    private Page getEmptyPage(TransactionId tid, Map<PageId, Page> dirtypages, int pgcateg)
            throws DbException, IOException, TransactionAbortedException {
        // create the new page
        int emptyPageNo = getEmptyPageNo(tid, dirtypages);
        BTreePageId newPageId = new BTreePageId(tableid, emptyPageNo, pgcateg);

        // write empty page to disk
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(BTreeRootPtrPage.getPageSize() + (long) (emptyPageNo - 1) * BufferPool.getPageSize());
        rf.write(BTreePage.createEmptyPageData());
        rf.close();

        // make sure the page is not in the buffer pool	or in the local cache
        Database.getBufferPool().discardPage(newPageId);
        dirtypages.remove(newPageId);

        return getPage(tid, dirtypages, newPageId, Permissions.READ_WRITE);
    }

    /**
     * Mark a page in this BTreeFile as empty. Find the corresponding header page
     * (create it if needed), and mark the corresponding slot in the header page as empty.
     *
     * @param tid         - the transaction id
     * @param dirtypages  - the list of dirty pages which should be updated with all new dirty pages
     * @param emptyPageNo - the page number of the empty page
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     * @see #getEmptyPage(TransactionId, Map, int)
     */
    public void setEmptyPage(TransactionId tid, Map<PageId, Page> dirtypages, int emptyPageNo)
            throws DbException, IOException, TransactionAbortedException {

        // if this is the last page in the file (and not the only page), just
        // truncate the file
        // @TODO: Commented out because we should probably do this somewhere else in case the transaction aborts....
//		synchronized(this) {
//			if(emptyPageNo == numPages()) {
//				if(emptyPageNo <= 1) {
//					// if this is the only page in the file, just return.
//					// It just means we have an empty root page
//					return;
//				}
//				long newSize = f.length() - BufferPool.getPageSize();
//				FileOutputStream fos = new FileOutputStream(f, true);
//				FileChannel fc = fos.getChannel();
//				fc.truncate(newSize);
//				fc.close();
//				fos.close();
//				return;
//			}
//		}

        // otherwise, get a read lock on the root pointer page and use it to locate
        // the first header page
        BTreeRootPtrPage rootPtr = getRootPtrPage(tid, dirtypages);
        BTreePageId headerId = rootPtr.getHeaderId();
        BTreePageId prevId = null;
        int headerPageCount = 0;

        // if there are no header pages, create the first header page and update
        // the header pointer in the BTreeRootPtrPage
        if (headerId == null) {
            rootPtr = (BTreeRootPtrPage) getPage(tid, dirtypages, BTreeRootPtrPage.getId(tableid), Permissions.READ_WRITE);

            BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
            headerId = headerPage.getId();
            headerPage.init();
            rootPtr.setHeaderId(headerId);
        }

        // iterate through all the existing header pages to find the one containing the slot
        // corresponding to emptyPageNo
        while (headerId != null && (headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
            BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_ONLY);
            prevId = headerId;
            headerId = headerPage.getNextPageId();
            headerPageCount++;
        }

        // at this point headerId should either be null or set with
        // the headerPage containing the slot corresponding to emptyPageNo.
        // Add header pages until we have one with a slot corresponding to emptyPageNo
        while ((headerPageCount + 1) * BTreeHeaderPage.getNumSlots() < emptyPageNo) {
            BTreeHeaderPage prevPage = (BTreeHeaderPage) getPage(tid, dirtypages, prevId, Permissions.READ_WRITE);

            BTreeHeaderPage headerPage = (BTreeHeaderPage) getEmptyPage(tid, dirtypages, BTreePageId.HEADER);
            headerId = headerPage.getId();
            headerPage.init();
            headerPage.setPrevPageId(prevId);
            prevPage.setNextPageId(headerId);

            headerPageCount++;
            prevId = headerId;
        }

        // now headerId should be set with the headerPage containing the slot corresponding to
        // emptyPageNo
        BTreeHeaderPage headerPage = (BTreeHeaderPage) getPage(tid, dirtypages, headerId, Permissions.READ_WRITE);
        int emptySlot = emptyPageNo - headerPageCount * BTreeHeaderPage.getNumSlots();
        headerPage.markSlotUsed(emptySlot, false);
    }

    /**
     * get the specified tuples from the file based on its IndexPredicate value on
     * behalf of the specified transaction. This method will acquire a read lock on
     * the affected pages of the file, and may block until the lock can be
     * acquired.
     *
     * @param tid   - the transaction id
     * @param ipred - the index predicate value to filter on
     * @return an iterator for the filtered tuples
     */
    public DbFileIterator indexIterator(TransactionId tid, IndexPredicate ipred) {
        return new BTreeSearchIterator(this, tid, ipred);
    }

    /**
     * Get an iterator for all tuples in this B+ tree file in sorted order. This method
     * will acquire a read lock on the affected pages of the file, and may block until
     * the lock can be acquired.
     *
     * @param tid - the transaction id
     * @return an iterator for all the tuples in this file
     */
    public DbFileIterator iterator(TransactionId tid) {
        return new BTreeFileIterator(this, tid);
    }

}

/**
 * Helper class that implements the Java Iterator for tuples on a BTreeFile
 */
class BTreeFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    BTreeLeafPage curp = null;

    final TransactionId tid;
    final BTreeFile f;

    /**
     * Constructor for this iterator
     *
     * @param f   - the BTreeFile containing the tuples
     * @param tid - the transaction id
     */
    public BTreeFileIterator(BTreeFile f, TransactionId tid) {
        this.f = f;
        this.tid = tid;
    }

    /**
     * Open this iterator by getting an iterator on the first leaf page
     */
    public void open() throws DbException, TransactionAbortedException {
        BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
                tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
        BTreePageId root = rootPtr.getRootId();
        curp = f.findLeafPage(tid, root, null);
        it = curp.iterator();
    }

    /**
     * Read the next tuple either from the current page if it has more tuples or
     * from the next page by following the right sibling pointer.
     *
     * @return the next tuple, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext())
            it = null;

        while (it == null && curp != null) {
            BTreePageId nextp = curp.getRightSiblingId();
            if (nextp == null) {
                curp = null;
            } else {
                curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
                        nextp, Permissions.READ_ONLY);
                it = curp.iterator();
                if (!it.hasNext())
                    it = null;
            }
        }

        if (it == null)
            return null;
        return it.next();
    }

    /**
     * rewind this iterator back to the beginning of the tuples
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * close the iterator
     */
    public void close() {
        super.close();
        it = null;
        curp = null;
    }
}

/**
 * Helper class that implements the DbFileIterator for search tuples on a
 * B+ Tree File
 */
class BTreeSearchIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    BTreeLeafPage curp = null;

    final TransactionId tid;
    final BTreeFile f;
    final IndexPredicate ipred;

    /**
     * Constructor for this iterator
     *
     * @param f     - the BTreeFile containing the tuples
     * @param tid   - the transaction id
     * @param ipred - the predicate to filter on
     */
    public BTreeSearchIterator(BTreeFile f, TransactionId tid, IndexPredicate ipred) {
        this.f = f;
        this.tid = tid;
        this.ipred = ipred;
    }

    /**
     * Open this iterator by getting an iterator on the first leaf page applicable
     * for the given predicate operation
     */
    public void open() throws DbException, TransactionAbortedException {
        BTreeRootPtrPage rootPtr = (BTreeRootPtrPage) Database.getBufferPool().getPage(
                tid, BTreeRootPtrPage.getId(f.getId()), Permissions.READ_ONLY);
        BTreePageId root = rootPtr.getRootId();
        if (ipred.getOp() == Op.EQUALS || ipred.getOp() == Op.GREATER_THAN
                || ipred.getOp() == Op.GREATER_THAN_OR_EQ) {
            curp = f.findLeafPage(tid, root, ipred.getField());
        } else {
            curp = f.findLeafPage(tid, root, null);
        }
        it = curp.iterator();
    }

    /**
     * Read the next tuple either from the current page if it has more tuples matching
     * the predicate or from the next page by following the right sibling pointer.
     *
     * @return the next tuple matching the predicate, or null if none exists
     */
    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException,
            NoSuchElementException {
        while (it != null) {

            while (it.hasNext()) {
                Tuple t = it.next();
                if (t.getField(f.keyField()).compare(ipred.getOp(), ipred.getField())) {
                    return t;
                } else if (ipred.getOp() == Op.LESS_THAN || ipred.getOp() == Op.LESS_THAN_OR_EQ) {
                    // if the predicate was not satisfied and the operation is less than, we have
                    // hit the end
                    return null;
                } else if (ipred.getOp() == Op.EQUALS &&
                        t.getField(f.keyField()).compare(Op.GREATER_THAN, ipred.getField())) {
                    // if the tuple is now greater than the field passed in and the operation
                    // is equals, we have reached the end
                    return null;
                }
            }

            BTreePageId nextp = curp.getRightSiblingId();
            // if there are no more pages to the right, end the iteration
            if (nextp == null) {
                return null;
            } else {
                curp = (BTreeLeafPage) Database.getBufferPool().getPage(tid,
                        nextp, Permissions.READ_ONLY);
                it = curp.iterator();
            }
        }

        return null;
    }

    /**
     * rewind this iterator back to the beginning of the tuples
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * close the iterator
     */
    public void close() {
        super.close();
        it = null;
    }
}
