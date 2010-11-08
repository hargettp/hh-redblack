(in-package :rb-tree)

;; =====================================================================================================================
;;
;; persistent red-black tree
;;
;; ---------------------------------------------------------------------------------------------------------------------


;; ---------------------------------------------------------------------------------------------------------------------
;; types
;; ---------------------------------------------------------------------------------------------------------------------

(defclass persistent-red-black-node (red-black-node)
  ())

(defclass persistent-red-black-tree (red-black-tree)
  ((storage :initarg :storage :accessor storage)))

(defclass red-black-tree-storage ()
  ())

(defclass red-black-tree-memory-storage (red-black-tree-storage)
  ((objects :initform (make-array 0) :accessor objects)))

(defclass red-black-tree-file-storage (red-black-tree-storage)
  ((file-name :initarg :file-name :accessor file-name)))

(defclass red-black-tree-transaction ()
  ((tree :initarg :tree :accessor tree) 
   (root :accessor root :documentation "Contains any new root for the tree, if root is changed")
   (opened :initform (make-hash-table :test #'equal) :accessor opened)
   (changed :initform (make-hash-table :test #'equal) :accessor changed)))

;; ---------------------------------------------------------------------------------------------------------------------
;; variables
;; ---------------------------------------------------------------------------------------------------------------------

(defvar *rb-transaction* nil 
  "The currently active transaction on a red-black tree")

;; ---------------------------------------------------------------------------------------------------------------------
;; generics
;; ---------------------------------------------------------------------------------------------------------------------

(defgeneric +opened (node))

(defgeneric +changed (node))

(defgeneric prb-open-storage (storage)
  (:documentation "Prepare storage for use; after this call load & save operations should succeed"))

(defgeneric prb-close-storage (storage)
  (:documentation "Release storage from use; further load & save operations cannot succeed without a subsequent open call"))

(defgeneric prb-load (storage object)
  (:documentation "Load the indicated object from storage (usually a node or tree)"))

(defgeneric prb-save (storage object)
  (:documentation "Save the indicated object in storage (usually a node or tree); 
    return a reference to its location within the storage"))

(defgeneric prb-commit (transaction-or-tree)
  (:documentation "Orchestrate the persisting of all changes to a tree, including all changed nodes"))

(defgeneric prb-abort (transaction-or-tree)
  (:documentation "Abandon any changes in the tree; note that any nodes held should be reacquired after an abort"))

;; ---------------------------------------------------------------------------------------------------------------------
;; implementation
;; ---------------------------------------------------------------------------------------------------------------------

(defmacro with-rb-transaction (() &rest body)
  `(let ((*rb-transaction* (make-instance 'red-black-tree-transaction)))
     (handler-case (let ((v (multiple-value-list (progn ,@body))))
		     (prb-commit *rb-transaction*)
		     (values-list v))
       (error () (progn 
		   (prb-abort *rb-transaction*)
		   nil)))))

(defun make-persistent-red-black-tree ()
  (with-rb-transaction ()
    (make-instance 'persistent-red-black-tree)))

(defmethod (setf leaf) :around (leaf (tree persistent-red-black-tree))
  (with-slots (storage) tree
    (let ((location (prb-save storage leaf)))
      (call-next-method location tree))))

(defmethod leaf :around ((tree persistent-red-black-tree))
  (with-slots (storage) tree
    (let ((leaf (prb-load storage (call-next-method tree))))
      leaf)))

(defmethod (setf root) :around (root (tree persistent-red-black-tree))
  (with-slots (storage) tree
    (let ((new-root (prb-save storage root)))
      (setf (root *rb-transaction*) new-root))))

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

(defmethod prb-load ((storage red-black-tree-memory-storage) (tree persistent-red-black-tree))
  (when (= 0 (length (objects storage)))
    ;; storage is empty, so pre-initialize the tree, leaf, and root node storage
    ))
