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
   (next-location :initform 0 :accessor next-location)))

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
   (object-to-location :initform (make-hash-table)  :accessor object-to-location)
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

(defgeneric allocate-location (transaction obj))

(defgeneric object-for-location (transaction location))

(defgeneric location-for-object (transaction obj))

(defgeneric add-changed-node (transaction node))

(defgeneric add-opened-node (transaction node location))

(defgeneric prb-open-storage (storage)
  (:documentation "Prepare storage for use; after this call load & save operations should succeed"))

(defgeneric prb-close-storage (storage)
  (:documentation "Release storage from use; further load & save operations cannot succeed without a subsequent open call"))

(defgeneric prb-load (storage location)
  (:documentation "Load the indicated object from storage (usually a node or tree)"))

(defgeneric prb-save (storage location object)
  (:documentation "Save the indicated object in storage (usually a node or tree); 
    return a reference to its location within the storage"))

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
		  (let ((location (call-next-method)))
		    (or (object-for-location *rb-transaction* location)
			(let ((node (prb-load (storage (tree *rb-transaction*)) location)))
			  (add-opened-node *rb-transaction* node location)
			  node))))

		(defmethod (setf ,slot) :around (value (node persistent-red-black-node))
		  (require-rb-transaction)
		  (add-changed-node *rb-transaction* node)
		  ;; (format *standard-output* "Setting slot ~s with value ~s~%" ',slot (location-for-object *rb-transaction* value))
		  (call-next-method (location-for-object *rb-transaction* value) node)))))
  (declare-slot-translation parent)
  (declare-slot-translation left)
  (declare-slot-translation right))

(defmethod (setf color) (color (node persistent-red-black-node))
  (require-rb-transaction)
  (add-changed-node *rb-transaction* node)
  (call-next-method))

(defmethod root :around ((tree persistent-red-black-tree))
  (require-rb-transaction)
  (let ((location (or (and *rb-transaction* (new-root *rb-transaction*)) (call-next-method))))
    (or (object-for-location *rb-transaction* location)
	(let ((root (prb-load (storage tree) location)))
	  (add-opened-node *rb-transaction* root location)
	  root))))

(defmethod (setf root) (node (tree persistent-red-black-tree))
  (require-rb-transaction)
  (add-changed-node *rb-transaction* node)
  (setf (new-root *rb-transaction*) (location-for-object *rb-transaction* node)))

(defmethod allocate-location ((*rb-transaction* red-black-tree-transaction) obj)
  (allocate-location (storage (tree *rb-transaction*)) obj))

(defmethod allocate-location ((storage red-black-tree-memory-storage) obj)
  (declare (ignorable obj))
  (let ((next-location (next-location storage)))
    (incf (next-location storage))
    next-location))

(defmethod object-for-location ((*rb-transaction* red-black-tree-transaction) location)
  (gethash location (location-to-object *rb-transaction*)))

(defmethod location-for-object ((*rb-transaction* red-black-tree-transaction) obj)
  (let ((location (gethash obj (object-to-location *rb-transaction*))))
    (if location
	location
	(let ((new-location (allocate-location *rb-transaction* obj)))
	  (setf (gethash obj (object-to-location *rb-transaction*)) new-location)
	  (setf (gethash location (location-to-object *rb-transaction*)) obj)
	  new-location))))

(defmethod rb-make-node :around ((tree persistent-red-black-tree) &key ((:key key) nil) ((:data data) nil))
  (declare (ignorable key data))
  (require-rb-transaction)
  (let ((node (call-next-method)))
    (add-new-node *rb-transaction* node)
    node))

(defmethod add-changed-node ((*rb-transaction* red-black-tree-transaction) node)
  (unless (gethash (location-for-object *rb-transaction* node) (changes *rb-transaction*))
    (let ((old-location (location-for-object *rb-transaction* node)) 
	  (new-location (allocate-location *rb-transaction* node)))
      (setf (gethash node (object-to-location *rb-transaction*)) new-location)
      (setf (gethash new-location (location-to-object *rb-transaction*)) node)
      (setf (gethash new-location (changes *rb-transaction*)) old-location))))

(defmethod add-opened-node ((*rb-transaction* red-black-tree-transaction) node location)
  (setf (gethash node (object-to-location *rb-transaction*)) location)
  (setf (gethash location (location-to-object *rb-transaction*)) node))

(defmethod add-new-node ((*rb-transaction* red-black-tree-transaction) node)
  (let ((new-location (allocate-location *rb-transaction* node)))
    (setf (gethash node (object-to-location *rb-transaction*)) new-location)
    (setf (gethash new-location (location-to-object *rb-transaction*)) node)
    (setf (gethash new-location (changes *rb-transaction*)) new-location)))

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

(defmethod prb-load ((storage red-black-tree-memory-storage) location)
  (aref (objects storage) location))

(defmethod prb-save ((storage red-black-tree-memory-storage) location object)
  (with-slots (objects) storage
    (adjust-array objects (max (+ 1 location) (length objects)))
    (setf (aref objects location) object)))

(defmethod prb-abort ((*rb-transaction* red-black-tree-transaction))
  (setf (new-root *rb-transaction*) nil)
  (clrhash (object-to-location *rb-transaction*))
  (clrhash (location-to-object *rb-transaction*))
  (clrhash (changes *rb-transaction*)))

(defmethod prb-commit ((*rb-transaction* red-black-tree-transaction))  
  ;; (format *standard-output* "Commiting transaction~%")
  (let ((locations (sort (loop for location being the hash-keys of (changes *rb-transaction*) collect location)
			 #'<))
	(storage (storage (tree *rb-transaction*))))
    (loop for location in locations
       do (prb-save storage location (object-for-location *rb-transaction* location)))
    (when (new-root *rb-transaction*)
          (setf (root storage) (new-root *rb-transaction*))
      	  (setf (slot-value (tree *rb-transaction*) 'root) (new-root *rb-transaction*)))))