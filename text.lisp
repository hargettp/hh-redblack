(in-package :rb-tree)

;; ---------------------------------------------------------------------------------------------------------------------
;; file-based storage
;;
;; We're using a text-based file format.  Each file is structured as a sequence of forms, where each form is a single object 
;; (node or data), and the references between objects are in the form (form# offset) and the representation of such references
;; is always a fixed-size.  Accessing storage depends on all objects involved (both nodes and data) to have representations 
;; created by cl:print-object and readable by cl:read.
;;
;; The first 2 forms of the file are reserved for the header and it's backup; the 3rd form should contain
;; the "leaf node" representation, which in an empty tree would also be the root.  All forms are terminated by #\Newline,
;; although that also is a convenience, and not a required delimiter between forms (because the Lisp reader does not
;; require that)

;; TODO note that the use of 20-char wide columns is because (length (format nil (expot 2 64))) is 20

(defclass red-black-tree-file-storage (red-black-tree-storage)
  ((file-name :initarg :file-name :accessor file-name)
   (stream :initform nil :accessor storage-stream)
   (root :accessor root)
   (leaf :accessor leaf)
   (next-form-number :initform 0 :accessor next-form-number)))

(defgeneric equality (left right)
  (:method ((left t) (right t))
    (equal left right)))

(defclass storage-node ()
  ((left :initform nil :initarg :left :accessor left)
   (right :initform nil :initarg :right :accessor right)
   (color :initform nil :initarg :color :accessor color)
   (key :initform nil :initarg :key :accessor key)
   (data :initform nil :initarg :data :accessor data)))

(defmacro node (&key left right color key data)
  `(make-instance 'storage-node  :left ,left :right ,right :color ,color :key ,key :data ,data))

(defmethod print-object ((object storage-node) stream)
  (with-slots (left right color key data) object
    (format stream "~s" `(node :left ,left :right ,right :color ,color :key ,key :data ,data))))

(defclass storage-reference ()
  ((form-number :initform 0 :initarg :form :accessor form-number)
   (offset :initform 0 :initarg :offset :accessor offset)))

(defmacro ref (form offset)
  `(make-instance 'storage-reference :form ,form :offset ,offset))

(defmethod print-object ((object storage-reference) stream)
  (format stream "(REF ~20<~s~> ~20<~s~>)" (form-number object) (offset object)))

(defmethod equality ((left storage-reference) (right storage-reference))
  (and (equality (form-number left) (form-number right))
       (equality (offset left) (offset right))))

(defclass storage-header ()
  ((version :initform 0 :initarg :version :accessor version)
   (root :initform (make-instance 'storage-reference) :initarg :root :accessor root)
   (leaf :initform (make-instance 'storage-reference) :initarg :leaf :accessor leaf)
   (next-form-number :initform 0 :initarg :next :accessor next-form-number)))

(defmethod equality ((left storage-header) (right storage-header))
  (and (equality (version left) (version right))
       (equality (root left) (root right))
       (equality (leaf left) (leaf right))
       (equality (next-form-number left) (next-form-number right))))

(defmacro header (version root-reference leaf next)
  `(make-instance 'storage-header :version ,version :root ,root-reference :leaf ,leaf :next ,next))

(defmethod print-object ((object storage-header) stream)
  (format stream "(HEADER ~20<~s~> ~s ~s ~20<~s~>)" (version object) (root object) (leaf object) (next-form-number object)))

(defclass storage-form ()
  ((form-number :initarg :number :accessor form-number)
   (contents :initarg :contents :accessor contents)))

(defmethod print-object ((object storage-form) stream)
  (format stream "(~20<~s~> ~s)~%" (form-number object) (contents object)))

(defun read-stored-object (stream)
  (let ((*package* (symbol-package 'red-black-tree)))
    (eval (read stream nil))))

(defun write-stored-object (stream object)
  (let ((*package* (symbol-package 'red-black-tree)))
    (format stream "~s~%" object)))

(defun make-storage-header (storage root-reference)
  (make-instance 'storage-header :root root-reference :leaf (leaf storage) :next (next-form-number storage)))

(defmethod prb-open-storage ((storage red-black-tree-file-storage))
  "Open storage and move file-position to end of file for appending any new data"
  (labels ((open-storage-stream (storage)
	     (open (file-name storage) :direction :io :if-exists :overwrite :if-does-not-exist :create :element-type 'character))
	   (initialize-storage (storage)
	     "Called the first time a storage file is used"
	     (let* ((stream (open-storage-stream storage)) 
		    (leaf-location (make-instance 'storage-reference 
						  :form 0 
						  :offset (* 2 (allocation-size storage (make-instance 'storage-header)))) )
		    (leaf (make-instance 'storage-node)))
	       (with-slots (left right color key data) leaf
		 (setf left leaf-location
		       right leaf-location
		       key nil
		       data leaf-location))
	       (setf (storage-stream storage) stream
		     (slot-value storage 'root ) leaf-location
		     (slot-value storage 'leaf) leaf-location)
	       (let ((header (make-storage-header storage leaf-location)))
		 (file-position stream :start)
		 (write-stored-object stream header)
		 (write-stored-object stream header)
		 (file-position stream :end))
	       ))
	   (refresh-storage (storage header)
	     "Refresh the storage object's slots from the provided header"
	     (setf (root storage) (root header)
		   (leaf storage) (leaf header)
		   (next-form-number storage) (next-form-number header))
	     storage)
	   (recover-storage (storage)
	     "Check header and backup for consistency, repairing if necessary; note that
              recovery should be idempotent, and always run"
	     ;; TODO consider an abort if hit an exception in here	     
	     (let* ((stream (open-storage-stream storage)))
	       (file-position stream :start) ;; set to beginning to read header
	       (let ((header (read-stored-object stream))
		     (backup (read-stored-object stream)))
		 (file-position stream :end) ;; restore file position, hopefully to end
		 (if (equality header backup)
		     ;; intact; no recovery needed
		     (refresh-storage storage header)
		     ;; they do not match; attempt to recover
		     (progn
		       (refresh-storage storage backup)
		       (prb-set-root storage (root backup)))))
	       (setf (storage-stream storage) stream)
	       (file-position stream :end))))
    (if (probe-file (file-name storage))
      (recover-storage storage)
      (initialize-storage storage))))

(defmethod prb-close-storage ((storage red-black-tree-file-storage))
  (close (storage-stream storage)))

(defmethod prb-location ((storage red-black-tree-file-storage))
  (make-instance 'storage-reference :form (next-form-number storage) :offset (file-position (storage-stream storage)))
  ;; (file-position (storage-stream storage))
  )

(defmethod prb-next-location ((storage red-black-tree-file-storage) (location storage-reference) size)
  (make-instance 'storage-reference :form (1+ (form-number location)) :offset (+ size (offset location))))

;; (defmethod prb-location-hash-key ((location storage-reference))
;;   (list (form-number location) (offset location)))

(defmethod prb-get-root ((storage red-black-tree-file-storage))
  (root storage))

(defmethod prb-set-root ((storage red-black-tree-file-storage) root-reference)
  ;; we're actually going to use this method to write out the header and backup for the tree
  (let ((header (make-instance 'storage-header :root root-reference :leaf (leaf storage) :next (next-form-number storage)))
	(storage-stream (storage-stream storage)))
    (file-position storage-stream :start)
    (write-stored-object storage-stream header)
    (write-stored-object storage-stream header)
    (setf (root storage) root-reference)
    (file-position storage-stream :end)))

(defmethod prb-load ((storage red-black-tree-file-storage) (location storage-reference))
  (file-position (storage-stream storage) (offset location))
  (let ((object (read (storage-stream storage) nil nil)))
    (file-position (storage-stream storage) :end)
    (if (typep object 'storage-node)
	(with-slots (left right color key data) object
	  (make-instance 'persistent-red-black-node :left left :right right :color color :key key :data data))
	object)))

(defmethod prb-save ((storage red-black-tree-file-storage) (object persistent-red-black-node))
  (with-slots (left right color key data) object
    (call-next-method storage (make-instance 'storage-node :left left :right right :color color :key key :data data))))

(defmethod prb-save ((storage red-black-tree-file-storage) object)
  (let ((form (make-instance 'storage-form :number (next-form-number storage) :contents object))
	(stream (storage-stream storage)))
    (file-position stream :end)
    (write-stored-object stream form)
    (incf (next-form-number storage))))

(defmethod allocation-size ((storage red-black-tree-file-storage) object)
  ;; note we're counting on storage-forms to always have the same
  ;; base allocation size, independent of the value of :number
  (let ((form (make-instance 'storage-form :number 1 :contents object)))
    (file-string-length (storage-stream storage) 
			(with-output-to-string (os)
			  (write-stored-object os form)))))

(defmethod allocation-size ((storage red-black-tree-file-storage) (header storage-header))
  (length (with-output-to-string (os)
	    (write-stored-object os header))))