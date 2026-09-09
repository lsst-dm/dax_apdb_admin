Replica chunks
--------------

- replica chunks created by DiaSource deduplication (times are local), only for visit/detectors that were re-processed

  |   Chunk    |     Time            | Comment             |
  | ---------- | ------------------- | --------------------|
  | 1772503800 | 2026-03-02 18:10:00 | Only re-assign      |
  | 1772504400 | 2026-03-02 18:20:00 | Only close validity |
  | 1772716200 | 2026-03-05 05:10:00 | Both                |
  | 1774285800 | 2026-03-23 10:10:00 | Both                |
  | 1779394800 | 2026-05-21 13:20:00 | Both                |


- replica chunks created by initial processing of visit/detectors,
  all are done before first deduplication

  |   Chunk range         |     Time  range                           |
  | --------------------- | ----------------------------------------- |
  | 1771468200-1771488600 | 2026-02-18 18:30:00 - 2026-02-19 00:10:00 |
  | 1771552200-1771564200 | 2026-02-19 17:50:00 - 2026-02-19 21:10:00 |
  | 1771624200-1771637400 | 2026-02-20 13:50:00 - 2026-02-20 17:30:00 |
  | 1771813200-1771828200 | 2026-02-22 18:20:00 - 2026-02-22 22:30:00 |
  | 1771893000-1771915800 | 2026-02-23 16:30:00 - 2026-02-23 22:50:00 |


- replica chunks created by reprocessing of visit/detectors

  |   Chunk range         |     Time  range                           |
  | --------------------- | ----------------------------------------- |
  | 1771537800-1771552200 | 2026-02-19 13:50:00 - 2026-02-19 17:50:00 |
  | 1771623000-1771636800 | 2026-02-20 13:30:00 - 2026-02-20 17:20:00 |
  | 1771866000-1771878600 | 2026-02-23 09:00:00 - 2026-02-23 12:30:00 |
  | 1772203800-1772212800 | 2026-02-27 06:50:00 - 2026-02-27 09:20:00 |
  |                       |  (first deduplication runs at 2026-03-02 18:10) |
  | 1772570400-1772589000 | 2026-03-03 12:40:00 - 2026-03-03 17:50:00 |
  | 1772685600-1772708400 | 2026-03-04 20:40:00 - 2026-03-05 03:00:00 |
  |                       |  (second deduplication runs at 2026-03-05 05:10) |



Cases
-----

Definitions:
- Initial record - DiaSource record from DiaSourceChunk table that was created in the first processing of the corresponding visit/detector.
  For a given diaSourceId the initial record may not even exist (DiaSource appears only in re-processing).
- Reprocessed records - all records from the DiaSourceChunk tables that are not from the initial processing of visit/detector.
- Source record(s) - records from DiaSource table for a diaSourceId, there could be more than one because of partitioning.
- Reassignment record - update record corresponding to a diaSourceId.

Combinations of cases:

1. No initial record for a given diaSourceId:

   - have to drop all reprocessed records and reassignment records.

2. Only the initial record, but no reprocessed records:

   - keep initial record, no updates needed; keep reassign_records

3. No reassignments records for this diaSourceId and reprocessed records are identical to the initial

   - drop reprocessed records
   - no need to update source record

4. No reassignments records for this diaSourceId but reprocessed records are different in contents

   - if reprocessed diaObjectIds are the same as in initial record then drop reprocessed records
   - if diaObjectIds are different from initial record then drop records whose diaObjectId was invalidated

5. Single reassign record, comes before all reprocessed records

  - drop reprocessed records
  - keep initial record and reassign record
  - there is a complication for just four DiaSources
    - re-processed records moved by a tiny amount and they were associated to a different DiaObject
    - later de-duplication deleted DiaObject that an initial record was associated to (after re-assignment), but DiaSource was not re-associated because later copy was associated to a different DiaObject
    - the fix in this case is to re-associate initial record to DiaObject on the re-processed record, but that may break causality
    - alternative is to keep initial record associated with DiaObject that is invalidated.


6. One or more reassign record which come after all reprocessed records, and reprocessed records are identical to the initial

  - drop reprocessed records
  - keep initial record and reassign records

7. Reprocessed records are identical to initial except for diaObjectId. One reassign record which changes diaObjectId back to the inital

  - drop reprocessed records
  - keep initial record
  - drop reassign record

8. Reprocessed records are identical to initial except for diaObjectId. One reassign record which changes diaObjectId to something different

  - drop reprocessed record
  - keep initial record and reassign record

9. Initial record has diaObjectId=None (and ssObjectId not None), one reprocessed identical to initial except for diaObjectId and ssObjectId and one reassignment record:

  - drop reprocessed records
  - keep initial record
  - drop reassign record

10. A single reprocessed record, coordinates different from initial record, one or two reassignment records matching coordinates of one of replicas

   - drop reprocessed record
   - drop reassignment record matching reprocessed record
   - keep initial record
   - keep reassignment record matching initial record

11. A single reprocessed record, coordinates are identical to initial record, two reassignment records, both are later then reprocessed record:

   - drop reprocessed record
   - drop first reassignment record
   - keep initial record
   - keep second reassignment record

12. A single reprocessed record, coordinates are identical to initial record, two reassignment records, first is before reprocessed record, reassignment can be to the same or different DiaObject:

   - drop reprocessed record
   - keep initial record
   - keep first reassignment record
   - drop second reassignment record

DiaSources assigned to invalidated DiaObjects
---------------------------------------------

After all above fixes there is a small number of initial DiaSource records that are assigned to invalidated DiaObjects.
This is likely due to a variation of this scenario:

- Initial processing creates DiaSource and associates it to some DiaObject.
- Reprocessing creates DiaSource with the same ID at different coordinates associated with different DiaObject.
- DiaObject deduplication decides to eliminate DiaObject with the initial DiaSource association.
- Reprocessed DiaSource "hides" initial DiaSource, so deduplication does not see it and does not reassign it.

Here is the list of these DiaSources:

    Closed diaObjectId: 170032912556097678
      DiaSource: chunk=1771828200 id=170046118907347217 ra=148.79543052752135 dec=1.124901868165004 part=59526158 obj_id=170028514776973502 ss_id=None
    Closed diaObjectId: 313871014972882979
      DiaSource: chunk=1771558800 id=170032897244266602 ra=62.724261693035416 dec=-49.05700708410277 part=44484419 obj_id=313871014972882979 ss_id=None
    Closed diaObjectId: 313756673035468934
      DiaSource: chunk=1771558800 id=170032897244266575 ra=62.61214203182979 dec=-49.07971276707365 part=44484430 obj_id=313756673035468934 ss_id=None
    Closed diaObjectId: 313853501347725449
      DiaSource: chunk=1771558800 id=170032897244266603 ra=62.71818594850555 dec=-49.05183053185953 part=44484418 obj_id=313853501347725449 ss_id=None
    Closed diaObjectId: 170028485642813483
      DiaSource: chunk=1771558800 id=170032897244266553 ra=62.62802725987654 dec=-49.13248137292401 part=44484116 obj_id=170028485642813483 ss_id=None
    Closed diaObjectId: 313756671658164370
      DiaSource: chunk=1771558800 id=170032897244266624 ra=62.49921766304578 dec=-48.94502261992475 part=44484444 obj_id=313756671658164370 ss_id=None
    Closed diaObjectId: 313761042353618970
      DiaSource: chunk=1771558800 id=170032897244266599 ra=62.48209236292681 dec=-48.97853348970782 part=44484445 obj_id=313761042353618970 ss_id=None
    Closed diaObjectId: 313985346781577240
      DiaSource: chunk=1771813200 id=170046083783720963 ra=57.26549685208388 dec=-48.96335549807423 part=44632597 obj_id=313967752752136260 ss_id=None
    Closed diaObjectId: 313897383821836488
      DiaSource: chunk=1771813800 id=170046086465454207 ra=59.373385342360166 dec=-48.24487824896292 part=44644075 obj_id=313963359375982661 ss_id=None
    Closed diaObjectId: 313994144746831958
      DiaSource: chunk=1771897200 id=170050480619126966 ra=58.194382584492594 dec=-49.277995977479456 part=44632339 obj_id=313871014480052299 ss_id=None
    Closed diaObjectId: 313994144746831958
      DiaSource: chunk=1771897200 id=170050481696014426 ra=58.194339697138105 dec=-49.27804661683134 part=44632339 obj_id=313871014480052299 ss_id=None
