(in-package :rb-tree)

;; =====================================================================================================================
;;
;; persistent red-black tree
;;
;; ---------------------------------------------------------------------------------------------------------------------


;; ---------------------------------------------------------------------------------------------------------------------
;; types
;; ---------------------------------------------------------------------------------------------------------------------

(defclass red-black-tree-memory-storage (red-black-tree-storage)
  ((objects :initform (make-array 0 :adjustable t) :accessor objects)
   (root :accessor root)
   (next-id :initform 0 :accessor next-id)))

(defclass red-black-tree-file-storage (red-black-tree-storage)
  ((file-name :initarg :file-name :accessor file-name)))

(defclass persistent-red-black-node (red-black-node)
  ())

(defclass persistent-red-black-tree (red-black-tree)
  ((storage :initform (make-instance 'red-black-tree-memory-storage) :initarg :storage :accessor storage)))

(defclass red-black-tree-storage ()
  ())

(defclass red-black-tree-transaction ()
  ((tree :initarg :tree :accessor tree) 
   (new-root :initform nil :accessor new-root)
   (object-to-id :initform (make-hash-table)  :accessor object-to-id)
   (id-to-object :initform (make-hash-table)  :accessor id-to-object)
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

(defgeneric add-changed-node (transaction node))

(defgeneric add-opened-node (transaction node id))

(defgeneric prb-open-storage (storage)
  (:documentation "Prepare storage for use; after this call load & save operations should succeed"))

(defgeneric prb-close-storage (storage)
  (:documentation "Release storage from use; further load & save operations cannot succeed without a subsequent open call"))

(defgeneric prb-load (storage id)
  (:documentation "Load the indicated object from storage (usually a node or tree)"))

(defgeneric prb-save (storage id object)
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
			  (add-opened-node *rb-transaction* node id)
			  node))))

		(defmethod (setf ,slot) :around (value (node persistent-red-black-node))
		  (require-rb-transaction)
		  (add-changed-node *rb-transaction* node)
		  (call-next-method (id-for-object *rb-transaction* value) node)))))
  (declare-slot-translation parent)
  (declare-slot-translation left)
  (declare-slot-translation right))

(defmethod (setf color) (color (node persistent-red-black-node))
  (require-rb-transaction)
  (add-changed-node *rb-transaction* node)
  (call-next-method))

(defmethod root :around ((tree persistent-red-black-tree))
  (require-rb-transaction)
  (let ((id (or (and *rb-transaction* (new-root *rb-transaction*)) (call-next-method))))
    (or (object-for-id *rb-transaction* id)
	(let ((root (prb-load (storage tree) id)))
	  (add-opened-node *rb-transaction* root id)
	  root))))

(defmethod (setf root) (node (tree persistent-red-black-tree))
  (require-rb-transaction)
  (add-changed-node *rb-transaction* node)
  (setf (new-root *rb-transaction*) (id-for-object *rb-transaction* node)))

(defmethod allocate-id ((*rb-transaction* red-black-tree-transaction) obj)
  (allocate-id (storage (tree *rb-transaction*)) obj))

(defmethod allocate-id ((storage red-black-tree-memory-storage) obj)
  (declare (ignorable obj))
  (let ((next-id (next-id storage)))
    (incf (next-id storage))
    next-id))

(defmethod object-for-id ((*rb-transaction* red-black-tree-transaction) id)
  (gethash id (id-to-object *rb-transaction*)))

(defmethod id-for-object ((*rb-transaction* red-black-tree-transaction) obj)
  (let ((id (gethash obj (object-to-id *rb-transaction*))))
    (if id
	id
	(let ((new-id (allocate-id *rb-transaction* obj)))
	  (setf (gethash obj (object-to-id *rb-transaction*)) new-id)
	  (setf (gethash id (id-to-object *rb-transaction*)) obj)
	  new-id))))

(defmethod rb-make-node :around ((tree persistent-red-black-tree) &key ((:key key) nil) ((:data data) nil))
  (declare (ignorable key data))
  (require-rb-transaction)
  (let ((node (call-next-method)))
    (add-new-node *rb-transaction* node)
    node))

(defmethod add-changed-node ((*rb-transaction* red-black-tree-transaction) node)
  (unless (gethash (id-for-object *rb-transaction* node) (changes *rb-transaction*))
    (let ((old-id (id-for-object *rb-transaction* node)) 
	  (new-id (allocate-id *rb-transaction* node)))
      (setf (gethash node (object-to-id *rb-transaction*)) new-id)
      (setf (gethash new-id (id-to-object *rb-transaction*)) node)
      (setf (gethash new-id (changes *rb-transaction*)) old-id))))

(defmethod add-opened-node ((*rb-transaction* red-black-tree-transaction) node id)
  (setf (gethash node (object-to-id *rb-transaction*)) id)
  (setf (gethash id (id-to-object *rb-transaction*)) node))

(defmethod add-new-node ((*rb-transaction* red-black-tree-transaction) node)
  (let ((new-id (allocate-id *rb-transaction* node)))
    (setf (gethash node (object-to-id *rb-transaction*)) new-id)
    (setf (gethash new-id (id-to-object *rb-transaction*)) node)
    (setf (gethash new-id (changes *rb-transaction*)) new-id)))

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

(defun open-persistent-red-black-tree (storage-info)
  "Return a persisted red-black tree connected to its backing storage"
  (declare (ignorable storage-info))
  ;; TODO for now, we are using memory-based storage; we'll shift to file-based storage later
  (let* ((storage (make-instance 'red-black-tree-memory-storage))
	 (tree (make-instance 'persistent-red-black-tree :storage storage)))
    (prb-open-storage storage)
    (prb-load storage tree)
    tree))

(defmethod rb-node-class ((tree persistent-red-black-tree))
  'persistent-red-black-node)

(defmethod prb-open-storage ((storage red-black-tree-memory-storage))
  )

(defmethod prb-load ((storage red-black-tree-memory-storage) id)
  (aref (objects storage) id))

(defmethod prb-save ((storage red-black-tree-memory-storage) id object)
  (with-slots (objects) storage
    (adjust-array objects (max (+ 1 id) (length objects)))
    (setf (aref objects id) object)))

(defmethod prb-abort ((*rb-transaction* red-black-tree-transaction))
  (setf (new-root *rb-transaction*) nil)
  (clrhash (object-to-id *rb-transaction*))
  (clrhash (id-to-object *rb-transaction*))
  (clrhash (changes *rb-transaction*)))

(defmethod prb-commit ((*rb-transaction* red-black-tree-transaction))  
  (let ((ids (sort (loop for id being the hash-keys of (changes *rb-transaction*) collect id)
			 #'<))
	(storage (storage (tree *rb-transaction*))))
    (loop for id in ids
       do (prb-save storage id (object-for-id *rb-transaction* id)))
    (when (new-root *rb-transaction*)
          (setf (root storage) (new-root *rb-transaction*))
      	  (setf (slot-value (tree *rb-transaction*) 'root) (new-root *rb-transaction*)))))