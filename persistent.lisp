(in-package :rb-tree)

;; =====================================================================================================================
;;
;; persistent red-black tree
;;
;; ---------------------------------------------------------------------------------------------------------------------


;; ---------------------------------------------------------------------------------------------------------------------
;; types
;; ---------------------------------------------------------------------------------------------------------------------

(defclass red-black-tree-storage ()
  ())

(defclass red-black-tree-memory-storage (red-black-tree-storage)
  ((objects :initform (make-array 0 :adjustable t) :accessor objects)
   (root :accessor root)))

(defclass red-black-tree-file-storage (red-black-tree-storage)
  ((file-name :initarg :file-name :accessor file-name)))

(defclass persistent-red-black-node (red-black-node)
  ())

(defclass persistent-red-black-tree (red-black-tree)
  ((storage :initform (make-instance 'red-black-tree-memory-storage) :initarg :storage :accessor storage)
   (root :initform 0 :accessor root)
   ;; hard-coding it's value
   (leaf :initform 0 :accessor leaf)))

(defclass red-black-tree-transaction ()
  ((tree :initarg :tree :accessor tree) 
   (new-root :initform nil :accessor new-root)
   (next-id :initform -1 :accessor next-id)
   (next-location :initform nil :accessor next-location)
   (object-to-id :initform (make-hash-table)  :accessor object-to-id 
		 :documentation "Maps objects to their temporary ids, which they have before transaction commit assigns a 
                                 permanent locationin storage")
   (id-to-object :initform (make-hash-table)  :accessor id-to-object)
   (object-to-location :initform (make-hash-table)  :accessor object-to-location
		       		 :documentation "Maps objects to their permanent location in storage, which they have
                                                 if they already existed in storage, or after a commit")
   (location-to-object :initform (make-hash-table)  :accessor location-to-object)
   (changes :initform (make-hash-table) :accessor changes)))

;; ---------------------------------------------------------------------------------------------------------------------
;; variables
;; ---------------------------------------------------------------------------------------------------------------------

(defvar *rb-transaction* nil 
  "The currently active transaction on a red-black tree")

;; ---------------------------------------------------------------------------------------------------------------------
;; generics
;; ---------------------------------------------------------------------------------------------------------------------

(defgeneric allocate-id (transaction obj))

(defgeneric object-for-id (transaction id))

(defgeneric id-for-object (transaction obj))

(defgeneric object-for-location (transaction location))

(defgeneric location-for-object (transaction obj))

(defgeneric add-new-object (transaction object))

(defgeneric add-changed-object (transaction object))

(defgeneric add-opened-object (transaction object id))

(defgeneric prb-open-storage (storage)
  (:documentation "Prepare storage for use; after this call load & save operations should succeed"))

(defgeneric prb-close-storage (storage)
  (:documentation "Release storage from use; further load & save operations cannot succeed without a subsequent open call"))

(defgeneric prb-location (storage)
  (:documentation "Return the id immediately after the content in the storage"))

(defgeneric assign-location (storage *rb-transaction* object))

(defgeneric allocation-size (storage object)
  (:documentation "Compute the amount of space in the storage to allocate"))

(defgeneric prb-load (storage location)
  (:documentation "Load the object at the indicated id from storage (usually data or a node)"))

(defgeneric prb-get-root (storage)
  (:documentation "Return the root according to storage"))

(defgeneric prb-set-root (storage root)
  (:documentation "Set the root of  storage"))

(defgeneric prb-save (storage object)
  (:documentation "Save the indicated object in storage (usually a node or tree); 
    return a reference to its id within the storage"))

(defgeneric prb-commit (transaction-or-tree)
  (:documentation "Orchestrate the persisting of all changes to a tree, including all changed nodes"))

(defgeneric prb-abort (transaction-or-tree)
  (:documentation "Abandon any changes in the tree; note that any nodes held should be reacquired after an abort"))

;; ---------------------------------------------------------------------------------------------------------------------
;; implementation
;; ---------------------------------------------------------------------------------------------------------------------

(define-condition requires-red-black-transaction ()
  ()
  (:report (lambda (condition stream)
	     (declare (ignorable condition))
	    (format stream "Accessing a persistent red-black tree requires a transaction; wrap code in a with-rb-transaction form"))))

(defun require-rb-transaction ()
  (unless *rb-transaction*
    (error 'requires-red-black-transaction)))

(macrolet ((declare-slot-translation (slot)
	     `(progn
		(defmethod ,slot :around ((node persistent-red-black-node))
		  (require-rb-transaction)
		  (let ((id (call-next-method)))
		    (or (object-for-id *rb-transaction* id)
			(let ((node (prb-load (storage (tree *rb-transaction*)) id)))
			  (add-opened-object *rb-transaction* node id)
			  node))))

		(defmethod (setf ,slot) :around (value (node persistent-red-black-node))
		  (require-rb-transaction)
		  (add-changed-object *rb-transaction* node)
		  (call-next-method (id-for-object *rb-transaction* value) node)))))
  (declare-slot-translation parent)
  (declare-slot-translation left)
  (declare-slot-translation right)
  (declare-slot-translation data))

(defmethod (setf color) (color (node persistent-red-black-node))
  (require-rb-transaction)
  (add-changed-object *rb-transaction* node)
  (call-next-method))

(defmethod root :around ((tree persistent-red-black-tree))
  (require-rb-transaction)
  (let ((id (or (and *rb-transaction* (new-root *rb-transaction*)) 
		(call-next-method) ;; could be nil, so get from storage
		(setf (slot-value tree 'root) (prb-get-root (storage tree))))))
    (or (object-for-id *rb-transaction* id)
	(let ((root (prb-load (storage tree) id)))
	  (add-opened-object *rb-transaction* root id)
	  root))))

(defmethod (setf root) (node (tree persistent-red-black-tree))
  (require-rb-transaction)
  (add-changed-object *rb-transaction* node)
  (setf (new-root *rb-transaction*) (id-for-object *rb-transaction* node)))

(defmethod leaf :around ((tree persistent-red-black-tree))
  (require-rb-transaction)
  (let ((id (call-next-method)))
    (or (object-for-id *rb-transaction* id)
	(let ((leaf (prb-load (storage tree) id)))
	  (add-opened-object *rb-transaction* leaf id)
	  leaf))))

(defmethod (setf leaf) (node (tree persistent-red-black-tree))
  (require-rb-transaction)
  (add-new-object *rb-transaction* node)
  (add-changed-object *rb-transaction* node)
  ;; note we're hard-coding the location
  (setf (slot-value tree 'leaf) 0))


(defmethod allocate-id ((*rb-transaction* red-black-tree-transaction) obj)
  (let ((next-id (next-id *rb-transaction*)))
    ;; we use negative values to ensure no collision with existing, allocated 
    ;; locations in storage
    (decf (next-id *rb-transaction*))
    next-id))

(defmethod object-for-id ((*rb-transaction* red-black-tree-transaction) id)
  (gethash id (id-to-object *rb-transaction*)))

(defmethod id-for-object ((*rb-transaction* red-black-tree-transaction) obj)
  (or (gethash obj (object-to-id *rb-transaction*))
      (let ((new-id (allocate-id *rb-transaction* obj)))
	(setf (gethash obj (object-to-id *rb-transaction*)) new-id)
	(setf (gethash new-id (id-to-object *rb-transaction*)) obj)
	new-id)))

(defmethod object-for-location ((*rb-transaction* red-black-tree-transaction) location)
  (gethash location (location-to-object *rb-transaction*)))

(defmethod location-for-object ((*rb-transaction* red-black-tree-transaction) obj)
  (gethash obj (object-to-location *rb-transaction*)))

(defmethod rb-make-node :around ((tree persistent-red-black-tree) &key ((:key key) nil) ((:data data) nil))
  (declare (ignorable key data))
  (require-rb-transaction)
  (when data (add-new-object *rb-transaction* data))
  ;; hard-coding a zero here, on the expectation that the nil sentinel node is first...let's see if that works
  ;; theoretically, no node other than the sentinel node ever has nil data (and now it should have itself as data)
  (let ((node (call-next-method tree :key (or key 0) :data (if data (id-for-object *rb-transaction* data) 0))))
    (add-new-object *rb-transaction* node)
    node))

(defmethod add-new-object ((*rb-transaction* red-black-tree-transaction) object)
  (let ((new-id (id-for-object *rb-transaction* object)))
    (setf (gethash new-id (changes *rb-transaction*)) object))
  ;; (format *standard-output* "Change count is ~s~%" (hash-table-count (changes *rb-transaction*)))
  object)

(defmethod add-changed-object ((*rb-transaction* red-black-tree-transaction) object)
  ;; (when (and (typep object 'persistent-red-black-node)
  ;; 	     (= 2 (slot-value object 'key)))
  ;;   (break))
  (setf (gethash (id-for-object *rb-transaction* object) (changes *rb-transaction*)) 
	object))

(defmethod add-opened-object ((*rb-transaction* red-black-tree-transaction) object id)  
  (setf (gethash object (object-to-id *rb-transaction*)) id)
  (setf (gethash id (id-to-object *rb-transaction*)) object))

(defmacro with-rb-transaction ((tree) &rest body)
  `(let* ((existing-transaction *rb-transaction*) 
	  (*rb-transaction* (or existing-transaction (make-instance 'red-black-tree-transaction))))
     (handler-bind ((error #'(lambda (e)
			       (declare (ignorable e))
			       (prb-abort *rb-transaction*))))
       (let ((v (multiple-value-list (progn 
				       (setf (tree *rb-transaction*) ,tree)
				       ,@body))))
	 (unless existing-transaction (prb-commit *rb-transaction*))
	 (values-list v)))))

(defmethod initialize-instance :before ((tree persistent-red-black-tree)  &key)
  (setf (tree *rb-transaction*) tree))

(defun make-persistent-red-black-tree (&key ((:storage storage) (make-instance 'red-black-tree-memory-storage)))
  (with-rb-transaction ((make-instance 'persistent-red-black-tree :storage storage))))

(defmethod rb-node-class ((tree persistent-red-black-tree))
  'persistent-red-black-node)

(defmethod prb-open-storage ((storage red-black-tree-memory-storage))
  )

(defmethod prb-location ((storage red-black-tree-memory-storage))
  (length (objects storage)))

(defmethod assign-location ((storage red-black-tree-memory-storage) (*rb-transaction* red-black-tree-transaction) object)
  (let ((location (or (next-location *rb-transaction*) 
		      (setf (next-location *rb-transaction*) (prb-location storage)))))
    (incf (next-location *rb-transaction*) (allocation-size storage object))
    (setf (gethash location (location-to-object *rb-transaction*)) object)
    (setf (gethash object (object-to-location *rb-transaction*)) location)
    location))

(defmethod allocation-size ((storage red-black-tree-memory-storage) object)
  (declare (ignorable storage object))
  1)

(macrolet ((copy-node (dest-class source-node &rest slots)
	     `(let ((dest-node (make-instance ',dest-class :key (slot-value ,source-node 'key) :data (slot-value ,source-node 'data))))
		(setf ,@(loop for slot in slots
			   append `( (slot-value dest-node ',slot)
				     (slot-value ,source-node ',slot) )))
		dest-node)))

  (defmethod prb-load ((storage red-black-tree-memory-storage) id)
    (let ((stored-object (aref (objects storage) id)))
      ;; (format *standard-output* "Loading from ~s object ~s~%" id stored-object)
      (cond ((typep stored-object 'memory-red-black-node)
	     (copy-node persistent-red-black-node stored-object color parent left right))
	    (t ;; data
	     stored-object))))

  (defmethod prb-save ((storage red-black-tree-memory-storage) object)
    (with-slots (objects) storage
      (let ((stored-object (cond ((typep object 'persistent-red-black-node)
				  (copy-node memory-red-black-node object color parent left right))
				 (t ;; data
				  object)))
	    (location (prb-location storage)))
	(adjust-array objects (+ 1 location))
	;; (format *standard-output* "Saving to ~s object ~s~%" location stored-object)
	(setf (aref objects location) stored-object)))))

(defmethod prb-get-root ((storage red-black-tree-memory-storage))
  (root storage))

(defmethod prb-set-root ((storage red-black-tree-memory-storage) root)
  (setf (root storage) root))

(defmethod prb-abort ((*rb-transaction* red-black-tree-transaction))
  (setf (new-root *rb-transaction*) nil)
  (clrhash (object-to-id *rb-transaction*))
  (clrhash (id-to-object *rb-transaction*))
  (clrhash (changes *rb-transaction*)))

(defmethod prb-commit ((*rb-transaction* red-black-tree-transaction))  
  (let ((storage (storage (tree *rb-transaction*)))
	(new-root-location nil)
	(new-data-count 0)
	(new-node-count 0))

    ;; (format *standard-output* "Starting commit~%")

    ;; Automatically add the root to the set of changes, because it must always change
    ;; if there are any other changes
    (when (> (hash-table-count (changes *rb-transaction*)) 0)
      (add-changed-object *rb-transaction* (root (tree *rb-transaction*))))

    ;; expand set of changed nodes to include all ancestors of any changed nodes,
    ;; and repeat until unable to add more nodes to set
    ;; but we only do this when storage isn't empty, to avoid issues
    ;; during first initialization
    (when (> (prb-location storage) 0) 
      (loop for new-changes = (make-hash-table)
	 with changes = (changes *rb-transaction*)
	 do (loop for id being the hash-keys of changes
	       for object = (object-for-id *rb-transaction* id)
	       when (typep object 'red-black-node)
	       do (let ((parent (parent object))
			(tree (tree *rb-transaction*)))
		    (unless (eq (leaf tree) parent)
		      (let ((parent-id (id-for-object *rb-transaction* parent)))
			(unless (gethash parent-id changes)
			    ;; (format *standard-output* "Id ~s for object ~s already in changes~%" parent-id object)
			  (setf (gethash parent-id new-changes) parent))))))
	 until (= 0 (hash-table-count new-changes))
	 do (loop for changed-id being the hash-keys of new-changes
	       for changed-object = (gethash changed-id new-changes)
	       ;; do (format *standard-output* "Adding parent id ~s to set of changes~%" changed-id)
	       do (setf (gethash changed-id changes) changed-object))))

    ;; (format *standard-output* "Change count is ~s~%" (hash-table-count (changes *rb-transaction*)))
    ;; allocate locations for data
    (loop for id being the hash-keys of (changes *rb-transaction*)
       for object = (object-for-id *rb-transaction* id)
       ;; do (format *standard-output* "Reviewing for allocation ~s~%" object)
       unless (typep object 'red-black-node)
       do (progn 
	    (assign-location storage *rb-transaction* object)
	    (incf new-data-count)))
    ;; allocate locations for nodes
    (loop for id being the hash-keys of (changes *rb-transaction*)
       for object = (object-for-id *rb-transaction* id)
       ;; do (format *standard-output* "Reviewing for allocation ~s~%" object)
       when (typep object 'red-black-node)
       do (progn 
	    (assign-location storage *rb-transaction* object)
	    (incf new-node-count))
       when (eq object (root (tree *rb-transaction*)))
       do (let ((new-root (root (tree *rb-transaction*))))
	    (setf new-root-location (location-for-object *rb-transaction* new-root))
	    ;; (format *standard-output* "Found root; allocated location ~s~%" new-root-location)
	    ))
    ;; allocate location for new root, if necessary
    ;; (when (new-root *rb-transaction*)
    ;;   (let ((new-root (root (tree *rb-transaction*))))
    ;; 	(setf new-root-location (or (location-for-object *rb-transaction* new-root)
    ;; 				    (assign-location storage *rb-transaction* new-root)))
    ;; 	(format *standard-output* "Allocated location for root ~s at ~s~%" new-root new-root-location)))

    ;; save data  
    (loop for i from 1 to new-data-count
       for data = (object-for-location *rb-transaction* (prb-location storage))
       unless data do (error "Current storage location does not map to data")
       do (prb-save storage data))
    ;; save nodes
    (loop for i from 1 to new-node-count
       for node = (object-for-location *rb-transaction* (prb-location storage))
       unless node do (error "Current storage location does not map to a node")
       ;; note that we are updating the in-memory nodes we've used
       ;; it remains to be seen that that is the right choice
       do (prb-save storage
		    (macrolet ((map-slot-to-location (slot)
				 `(when (and (slot-boundp node ',slot)
					     (slot-value node ',slot)
					     ;; only when the slot-value is negative, and thus an id
					     (not (= 0 (slot-value node ',slot))))
				    (let* ((mapped-id (slot-value node ',slot))
					   (mapped-object (object-for-id *rb-transaction* mapped-id))
					   (mapped-location (if (> 0 mapped-id) 
								;; new object
								(location-for-object *rb-transaction* mapped-object)
								;; old object--but did it move?
								(or (location-for-object *rb-transaction* mapped-object) mapped-id))))
				      ;; (format *standard-output* "Mapped id ~s to location ~s for node ~s at ~s~%" mapped-id mapped-location node (prb-location storage))
				      (setf (slot-value node ',slot) mapped-location)))))
		      (map-slot-to-location parent)
		      (map-slot-to-location left)
		      (map-slot-to-location right)
		      (map-slot-to-location data)
		      node)))
    ;; save root--if we haven't already saved it
    (when (and new-root-location (equal new-root-location (prb-location storage)))
      ;; (format *standard-output* "Saving root ~s~%" (root (tree *rb-transaction*)))
      (prb-save storage (root (tree *rb-transaction*))))
    ;; (format *standard-output* "Would set for new root ~s~%" new-root-location)
    (when new-root-location
      (prb-set-root storage new-root-location)
      (setf (slot-value (tree *rb-transaction*) 'root) new-root-location))
    ;; (format *standard-output* "Finished commit~%")
    ))

;; ---------------------------------------------------------------------------------------------------------------------
;; printing

(defmethod print-object ((obj red-black-node) stream)
  (with-slots (parent left right color key data) obj
    (print-unreadable-object (obj stream :type t :identity t)
      (with-slots (parent left right color key data) obj      
	(if (eq obj parent)
	    (format stream "T.nil")
	    (format stream "Color=~s Key=~s Data=~s ~_Parent=~<~s~> ~_Left=~<~s~> ~_Right=~<~s~>" color key data parent left right))))))