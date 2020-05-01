//
//  FirebasePersistenceManager.swift
//  Graffiti
//
//  Created by adeiji on 4/6/18.
//  Copyright © 2018 Dephyned. All rights reserved.
//

import Foundation
import FirebaseFunctions
import UIKit
import FirebaseFirestore
import FirebaseAuth
import FirebaseStorage
import RxSwift
import RxCocoa
import AWSCore
import AWSS3
import GeoFire

/*
 
 This class handles all network request to Firebase Firestore and Google Docs
 
 */

let kDocumentId = "document_id"
let kUsersCollection = "users"
let kUserId = "userId"

open class FirebasePersistenceManager: NSObject {
    
    static var timer:Timer!
    
    /// Stores the image data all the images that have been downloaded from the server to this point
    static let imageCache = NSCache<NSString, NSData>()
    
    static let profileImageCache = NSCache<NSString, NSData>()
    
    let disposeBag = DisposeBag()
    
    var lastDocumentSnapshot:DocumentSnapshot?
    
    static let shared = FirebasePersistenceManager()
    
    private static var downsampleQueue = DispatchQueue(label: "downsample")
    
    
    /// The current logged in user's profile picture
    ///
    /// - Returns: A String object representing the current user's profile picture url or nil if a user is not logged in
    class func getProfilePictureURL () -> String? {
        if (Auth.auth().currentUser != nil) {
            return Auth.auth().currentUser?.photoURL?.absoluteString
        }
        
        return nil

    }
    
    
    /// Checks whether someone is logged in or not
    ///
    /// - Returns: True if a user is logged in, false if no user logged in
    class func isLoggedIn () -> Bool {
        return Auth.auth().currentUser?.displayName != nil
    }
    
    /**
     Delete an image from Amazon S3
     
     - parameters:
        - imageUrl: The url of the image to delete
        - bucketUrl: The url of the bucket to delete the image from
        - appBucket: The name of the bucket to delete the image from
     */
    class func deleteImage (imageUrl:URL?, bucketUrl:String, appBucket:String) {
        guard let imageUrl = imageUrl else { return }
        guard let deleteObjectRequest = AWSS3DeleteObjectRequest() else { return }
        guard let keyIndex = imageUrl.absoluteString.range(of: "\(bucketUrl)/")?.upperBound else { return }
        
        deleteObjectRequest.bucket = appBucket
        deleteObjectRequest.key = String(imageUrl.absoluteString.suffix(from: keyIndex))
        
        let s3 = AWSS3.default()
        s3.deleteObject(deleteObjectRequest)
    }
    
    /**
     Uploads an image to Amazon S3
     
     - parameters:
        - fullPath: The path of the image, it should start with the name of the folder, ie. images/wall-images/id
        - bucketUrl: The url of the bucket to upload the image to
        - appBucket: The name of the bucket to upload the image to
        - imageData: The image as data to store to the bucket
        - completion: Closure to execute on completion, return the URL of the image if uploaded successfully or an error
     */
    class func uploadImage (fullPath:String, bucketUrl:String, appBucket:String, imageData: Data, completion: @escaping (URL?, Error?) -> Void) {
        
        if let url = URL(string: "\(bucketUrl)/\(fullPath)") {
            self.saveImageToCache(url: url, data: imageData as NSData, isProfileImage: false)
        }
        
        let expression = AWSS3TransferUtilityUploadExpression()
        expression.progressBlock = {(task, progress) in
            DispatchQueue.main.async(execute: {
                // Do something e.g. Update a progress bar.
            })
        }
        
        var completionHandler: AWSS3TransferUtilityUploadCompletionHandlerBlock?
        completionHandler = { (task, error) -> Void in
            DispatchQueue.main.async(execute: {
                // Alert user for transfer completion
            })
        }
        
        let transferUtility = AWSS3TransferUtility.default()
        transferUtility.uploadData(imageData, bucket: appBucket, key: fullPath, contentType: "image/jpeg", expression: expression, completionHandler: completionHandler).continueWith { (task) -> Any? in
            if let error = task.error, task.isFaulted {
                print("Error: \(error.localizedDescription)")
                completion(nil, error)
            }
            
            if let _ = task.result {
                if task.isCompleted {
                    let imageUrl = URL(string: "\(bucketUrl)/\(fullPath)")!
                    completion(imageUrl, nil)
                }
            }
            
            return nil
        }
    }
    
    static func getDocumentById (forCollection collection: String, id: String) -> Observable<FirebaseDocument> {
                                
        return Observable.create { (observer) -> Disposable in
            let db = Firestore.firestore()
            let docRef = db.collection(collection).document(id)
            
            docRef.getDocument { (snapshot, error) in
                if let error = error {
                    observer.onError(error)
                }
                
                if let snapshot = snapshot {
                    let document = FirebasePersistenceManager.convertDocSnapshotToFirebaseDoc(document: snapshot)
                    observer.onNext(document)
                    observer.onCompleted()
                }                
            }
            
            return Disposables.create()
        }
    }
    
    /**
     Description Add a document with an image that is stored with Firebase in a Google Files Bucket
     - parameters:
        - fullPath: String The path to where you want to save ex: images/{UUID}/picture.jpg
        - imageData: Data The Data object for the image
        - bucketUrl: The url of the bucket to upload the image to
        - appBucket: The name of the bucket to upload the image to
        - collection: String The collection in database to store to
        - data: [String:Any] The document that you want to save
        - completion: FirebaseRequestClosure? The block to run amongst completion
    ````
    FirebasePersistenceManager.addDocumentWithImage(fullPath: "userId/\(kImagesUrl)/\(NSUUID().uuidString)", imageData: imageData, collection: kTagsCollection, data: data )
     ````
     */
    class func addDocumentWithImage (fullPath: String, bucketUrl:String, appBucket:String, imageData: Data, collection:String, data:[String:Any], withId id:String? = nil, completion: FirebaseRequestClosure?) {
        
        self.uploadImage(fullPath: fullPath, bucketUrl: bucketUrl, appBucket: appBucket, imageData: imageData) { (url, error) in
            
            var myData = data;
            myData["image_url"] = url?.absoluteString
            
            if let id = id {
                self.addDocument(withCollection: collection, data: myData, withId: id, completion: { (error, document) in
                    completion?(error, document)
                })
            } else {
                self.addDocument(withCollection: collection, data: myData, completion: { (error, document) in
                    completion?(error, document)
                })
            }
        }
    }
    
    private class func saveImageToCache (url: URL?, data:NSData?, isProfileImage:Bool) {
        if let url = url {
            if let data = data {
                if isProfileImage {
                    self.profileImageCache.setObject(data, forKey: url.absoluteString as NSString)
                } else {
                    self.imageCache.setObject(data, forKey: url.absoluteString as NSString)
                }                
            }
        }
    }
    
    class func getImageFromCache (url: URL?, completion: @escaping (Error?, UIImage?, String?) -> Void) {
        guard let url = url else { return }
        if let cachedImageData = self.imageCache.object(forKey: url.absoluteString as NSString) as Data? {
            completion(nil, UIImage(data: cachedImageData), url.absoluteString)
        } else {
            completion(nil, nil, url.absoluteString)
        }
    }
    
    /**
     Takes image data and decompresses is with the correct settings to decrease the size of the decompressed UIImage object
     */
    private class func downsampleImage (imageData: Data, maxImageWidth:CGFloat, completion: @escaping (CGImage?) -> Void) {
        
        self.downsampleQueue.async {
            let start = CFAbsoluteTimeGetCurrent()
            
            let options = [kCGImageSourceThumbnailMaxPixelSize: maxImageWidth, kCGImageSourceCreateThumbnailFromImageAlways: true] as CFDictionary
            
            guard let imageSource = CGImageSourceCreateWithData(imageData as NSData as CFData , options) else {
                completion(nil)
                return
            }
            
            if let scaledImage = CGImageSourceCreateThumbnailAtIndex(imageSource, 0, options) {
                let diff = CFAbsoluteTimeGetCurrent() - start
                print("GraffitiSocialLocation - downsample of image took \(diff) to run...")
                completion(scaledImage)
                return
            }
            
            completion(nil)
        }
    }
    
    /// Gets an image from a url
    ///
    /// - todo:
    /// If the user doesn't provide a URL than throw an error
    /// - Parameters:
    ///   - url: URL The url of the image
    ///   - completion: Closure of type <Error?, UIImage?>
    class func downloadImage (url: URL?, isProfileImage:Bool = false, imageWidth:CGFloat = 800, needImageReturned:Bool = true, completion: @escaping (Error?, UIImage?, String?) -> Void) {
        
        guard let url = url else { return }
        
        // If the image is not a profile image
        if !isProfileImage {
            // Check to see if this image has already been downloaded
            if let cachedImageData = self.imageCache.object(forKey: url.absoluteString as NSString) as Data? {
                // if the user just wants to download the image and not process it
                if needImageReturned == false {
                    completion(nil, nil, url.absoluteString)
                    return
                }
                
                self.downsampleImage(imageData: cachedImageData, maxImageWidth: imageWidth, completion: { cgImage in
                    guard let cgImage = cgImage else {
                        completion(nil, nil, url.absoluteString)
                        return
                    }
                    
                    completion(nil, UIImage(cgImage: cgImage), url.absoluteString)
                })
                
                return
            }
        } else { // If the image is a profile image and it's already cached, than just return the cached image
            if let cachedImageData = self.profileImageCache.object(forKey: url.absoluteString as NSString) as Data? {
                // if the user just wants to download the image and not process it
                if needImageReturned == false {
                    completion(nil, nil, url.absoluteString)
                    return
                }
                
                self.downsampleImage(imageData: cachedImageData, maxImageWidth: imageWidth, completion: { cgImage in
                     guard let cgImage = cgImage else {
                        completion(nil, nil, url.absoluteString)
                        return
                    }
                    
                     completion(nil, UIImage(cgImage: cgImage), url.absoluteString)
                     return
                 })
                
                return
            }
        }
        
    }
    
    func downloadImageFromHTTPS (url: URL, completion: @escaping (Error?, Data?) -> Void) {
        let session = URLSession(configuration: .default)
        
        //creating a dataTask
        let getImageFromUrl = session.dataTask(with: url) { (data, response, error) in
                                    
            //if there is any error
            if let e = error {
                //displaying the message
                print("Error Occurred: \(e)")
                DispatchQueue.main.sync {
                    completion(error, nil)
                }
            } else {
                //in case of now error, checking wheather the response is nil or not
                if (response as? HTTPURLResponse) != nil {
                    //checking if the response contains an image
                    DispatchQueue.main.sync {
                        completion(error, data)
                    }
                } else {
                    print("No response from server for https request: \(url.absoluteString)")
                }
            }
        }
        
        //starting the download task
        getImageFromUrl.resume()
    }
    
    func downloadImageData (url: URL?, bucketName:String? = nil, completion: @escaping (Error?, Data?, String?) -> Void) {
        
        guard let url = url else { return }
            
        if url.absoluteString.range(of: "firebasestorage") == nil {
            self.downloadImageFromHTTPS(url: url) { (error, data) in
                if let error = error {
                    print("Error downloading image with error - \(error.localizedDescription)")
                } else {
                    //                    self.saveImageToCache(url: url, image: image)
                    completion(error, data, url.absoluteString)
                    
                }
            }
        } else {
            guard let bucketName = bucketName else {
                assertionFailure("Make sure the bucketName is set if you're using firebasestorage")
                return
            }
            // Reference from a Google Cloud Storage URI
            let storage = Storage.storage(url: "gs://\(bucketName)")
            let gsReference = storage.reference(forURL: url.absoluteString)
            gsReference.getData(maxSize: 1 * 1024 * 1024) { (data, error) in
                if let error = error {
                    // Uh-oh, an error occurred!
                    print("Error downloading image at url \(url): \(error)")
                    completion(error, nil, url.absoluteString)
                } else {
                        //                        self.saveImageToCache(url: url, image: image)
                    completion(error, data, url.absoluteString)
                    
                }
            }
        }
        
    }
    
    /**
     Adds a document to the Firebase Firestore with given
     - parameters:
        - collection: Name of the collection in Firebase database
        - data: The data to store in the Database
        - id: (Optional) The id that you want to set for the document's id.  If this is set than setData will be called as opposed to addDocument.  See Firebase Firestore docs for info on the difference between setData and addDocument
        - shouldMerge: (Optional) Boolean value indicating whether we should overwrite the data if it exists on the server or merge the data.  If shouldMerge is set to true, make sure that you also provide an id
        - completion: The closure to get results, of type FirebaseRequestClosure? Returns the created document
     */
    class func addDocument (withCollection collection:String, data:[String:Any], withId id:String? = nil, shouldMerge:Bool = false, completion: FirebaseRequestClosure?) {
        let db = Firestore.firestore()
        var ref: DocumentReference? = nil
        self.startTimer()
        
        if id == nil {
            ref = db.collection(collection).addDocument(data: data) { (err) in
                if let err = err {
                    print ("Error adding document to collection \(collection): \(err)" )
                    return
                } else {
                    print("Document added to collection \(collection) with ID: \(ref!.documentID)")
                    completion?(nil, FirebaseDocument(documentId: ref!.documentID, data: data))
                }
            }
        } else {
            if shouldMerge {
                if id == nil {
                    fatalError("If you're going to merge than you need to make sure that you set the id, ex: withId: (id)")
                } else {
                    db.collection(collection).document(id!).setData(data, merge: true) { err in
                        if let err = err {
                            completion?(err, nil)
                        } else {
                            completion?(nil, FirebaseDocument(documentId: id!, data: data))
                        }
                    }
                }
            } else {
                db.collection(collection).document(id!).setData(data) { err in
                    if let err = err {
                        completion?(err, nil)
                    } else {
                        completion?(nil, FirebaseDocument(documentId: id!, data: data))
                    }
                }
            }
        }
    }

    class private func startTimer () {
        if self.timer != nil {
            self.timer.invalidate()
        }
        self.timer = Timer.scheduledTimer(timeInterval: 3.0, target: self, selector: #selector(multipleWriteFinished), userInfo: nil, repeats: false)
    }
    
    @objc class func multipleWriteFinished () {
        // Show the finished banner
//        UtilityFunctions.showTaskCompleted(title: "Finished Importing From Instagram")
    }
    
    /**
    Check to see if this document already has a document with the given key value.  If it's not a duplicate than it will be added to the db
     - parameters:
        - collection The collection to check
        - key - The key in the collection to see if is duplicate
        - value - The value of the key to check to see if is duplicate
        - document - The document that you want to store in firestore
        - id - (Optional) The id that you want to set for the document
        - completion - Closure of type <Error?, FirebaseDocument?>
     */
    class func addDocumentIfNotDuplicate (withCollection collection: String, key: String, value: Any, document:[String:Any], withId id:String? = nil, completion: FirebaseRequestClosure?) {
        let db = Firestore.firestore()
        let docRef = db.collection(collection).whereField(key, isEqualTo: value)
                
        docRef.getDocuments { (querySnapshot, err) in
            if querySnapshot?.documents.count == 0 {
                FirebasePersistenceManager.addDocument(withCollection: collection, data: document, withId: id, completion: { (error, document
                    ) in
                    completion?(error, document)
                })
            } else {
                completion?(nil, nil)
            }
        }
    }
    
    /**
     Converts a dictionary of data which contains user data into a FirebaseDocument
     - Parameters:
        - docsWithUser: The HTTPSCallableResult which contains the user document information
     - Returns:
        - An array of FirebaseDocuments which contains the user data as well
     */
    class private func convertUserDocumentsArrayToFirebaseDocuments (docsWithUser: HTTPSCallableResult) -> [FirebaseDocument]? {
        var firebaseDocuments = [FirebaseDocument]()
        
        // First check to see if the Document structure is of type Array of Dictionaries
        var docWithUserDataArray = docsWithUser.data as? [[String:Any]]
        
        // If it's not of type Array of Dictionaries than get the value for key tags which should contain a type Dictionary of arrays
        if docWithUserDataArray == nil {            
            if let dict = docsWithUser.data as? [String:Any] {
                if dict.keys.count == 0 {
                    return firebaseDocuments
                }
                
                docWithUserDataArray = dict["tags"] as? [[String:Any]]
                // This means the data is all in the wrong format
                if docWithUserDataArray == nil {
                    fatalError("If the document is in the incorrect format, this is a very big problem because than we can't parse it properly")
                }
            } else {
                return firebaseDocuments
            }
        }
        
        if let docWithUserDataArray = docWithUserDataArray {
            for docWithUserData in docWithUserDataArray {
                let user = docWithUserData["user"] as! [String:Any]
                var actualDocument = docWithUserData["document"] as! [String:Any]
                actualDocument["user"] = user
                if let documentId = actualDocument[kDocumentId] as? String {
                    let firebaseDocument = FirebaseDocument(documentId: documentId, data: actualDocument)
                    firebaseDocuments.append(firebaseDocument)
                }
            }
        }
        
        return firebaseDocuments
        
    }
    
    
    /// Deletes documents from a Firebase collection
    ///
    /// - Parameters:
    ///   - collection: The collections which to
    ///   - queryDocument: The key, values of the field you want deleted
    ///   - documentId: Optional String - The ID of the document that you want to delete if that is known
    class func deleteDocuments (withCollection collection: String, queryDocument:[String:Any]? = nil, documentId:String? = nil, completion: ((Bool, Error?) -> Void)? = nil)  {
        if let documentId = documentId {
            let db = Firestore.firestore()
            db.collection(collection).document(documentId).delete() { err in
                if let err = err {
                    print("Error removing document from collection \(collection) with id \(documentId): \(err)")
                    completion?(false, err)
                } else {
                    completion?(true, nil)
                    print("Document successfully removed!")
                }
            }
        }
    }
    
    /// Takes a dictionary of key values and turns it into a FirebaseKeyValues object which consists of a keys of type [String] and values of type [String]
    ///
    /// - Parameter queryDocument: The key values as a dictionary
    /// - Returns: FirebaseKeyValues - The queryDocument converted into seperate key value arrays
    private class func getKeyValues (queryDocument:[String:Any]) -> FirebaseKeyValues {
        var keys = [String]()
        var values = [Any]()
        for key in queryDocument.keys {
            keys.append(key)
            values.append(queryDocument[key]!)
        }
        
        return FirebaseKeyValues(keys: keys, values: values)
    }
    
    
    /// Gets documents from a collection that match multiple key values
    ///
    /// - Parameters:
    ///   - collection: String The collection of which to search on
    ///   - queryDocument: [String:Any] The object containing the key values to search on
    ///   - completion: A closure of type FirebaseRequestMultiDocClosure - contains the documents received from Firebase Firestore collection
    class func queryWithMultipleKeyValues (withCollection collection: String, queryDocument:[String:Any], fromCache:Bool = false, completion: @escaping FirebaseRequestMultiDocClosure) {
        let db = Firestore.firestore()
        let collectionRef = db.collection(collection)
        var query:Query!
        
        for key in queryDocument.keys {
            query = collectionRef.whereField(key, isEqualTo: queryDocument[key]!)
        }
        
        query.getDocuments { (querySnapshot, err) in
            if let err = err {
                print("FirebasePersistenceManager.queryWithMultipleKeyValues Error retrieving documents with error: \(err.localizedDescription)")
                completion(err, nil)
            } else {
                completion(nil, convertSnapshotToFirebaseDocuments(querySnapshot: querySnapshot!))
            }
        }
    }
    
    /**
    Converts the data from a QuerySnapshot into a List of FirebaseDocuments
     
     - parameters querySnapshot: The snapshot to get the data from
     - returns: An array of FirebaseDocuments
     */
    class func convertSnapshotToFirebaseDocuments (querySnapshot: QuerySnapshot) -> [FirebaseDocument] {
        var documents = [FirebaseDocument]()
        for doc in querySnapshot.documents {
            documents.append(FirebaseDocument(documentId: doc.documentID, data: doc.data()))
        }
        return documents
    }
    
    /**
     Gets a Firebase Query which will return a document that contains a string
     
     When wanting to search by substring on Firebase you have to check for values from a start point to an end point. So for example, if you want all the values that start with "a", you have to search for all values >= a && <= b
     
     So in this function we get the next letter and get all the values within the range
     
     - Parameters:
        - value: The value of the key of the Firestore collection to perform the search on
        - collection: The Firestore collection to search on
        - key: The key in the Firestore collection to perform the search on
     */
    class func getSearchQuery(value:String, collection:String, key:String) -> Query {
        let db = Firestore.firestore()
        var myValue = value
        let lastCharacter = myValue.last
        let nextLetter = self.nextLetter(String(describing: lastCharacter!).lowercased())
        
        myValue.removeLast()
        myValue = myValue + (nextLetter ?? "")
                        
        let docRef = db.collection(collection).whereField(key, isGreaterThanOrEqualTo: value.lowercased()).whereField(key, isLessThan: myValue.lowercased())
        return docRef
    }
    
    /// Gets the next letter after a given letter.  We use this method so that we can make sure that our searches are not case sensitive
    class func nextLetter(_ letter: String) -> String? {
        // Check if string is build from exactly one Unicode scalar:
        guard let uniCode = UnicodeScalar(letter) else {
            return nil
        }
        switch uniCode {
        case "a" ..< "z":
            return String(UnicodeScalar(uniCode.value + 1)!)
        default:
            if (letter.range(of: "\\P{Latin}", options: .regularExpression) != nil) {
                if let nextCharacterValue = UnicodeScalar(uniCode.value + 1) {
                    return String(nextCharacterValue)
                }
            }
            
            // If there is no next letter that means that we're at the end of the alphabet, so return "{" which is the next character in Unicode
            return "{"
        }
    }
    
    /**
     Gets an observable for querying a firestore collection with a given key and value.  Retrieves document if value starts with given the value
     
     - parameters:
        - value: The value for the key
        - key: The key for the key-value pair to check
        - collection: The Firestore collection to perform the query  on
     
     - returns: An observable for the query that can be subscribed to
     */
    class func searchForDocumentsAsObservable (value:String, collection:String, key:String) -> Observable<[FirebaseDocument]> {
        
        return Observable.create { (observer) -> Disposable in
            let docRef = getSearchQuery(value: value, collection: collection, key: key)
            docRef.getDocuments { (snapshot, error) in
                if let error = error {
                    observer.onError(error)
                }
                else if let snapshot = snapshot {
                    let documents = convertSnapshotToFirebaseDocuments(querySnapshot: snapshot)
                    
                    observer.onNext(documents)
                    observer.onCompleted()
                }
            }
            
            return Disposables.create()
        }
    }
    
    /**
     Gets all the documents from a Firestore collection up to a certain amount
     
     - Parameters:
        - count: The number of documents to get
        - collection: The Firestore collection to search on
        - completion: The closure to execute upon response
     */
    class func getDocuments (count: Int? = nil, collection: String, completion: @escaping FirebaseRequestMultiDocClosure) {
        let db = Firestore.firestore()
        var docRef:Query!
        
        docRef = db.collection(collection)
        if let count = count {
            docRef.limit(to: count)
        }
        
        docRef.getDocuments { (querySnapshot, err) in
            docRef.getDocuments { (querySnapshot, err) in
                if let err = err {
                    print("Error getting documents: \(err)")
                    completion(err, nil)
                } else {
                    var documents = [FirebaseDocument]()
                    for doc in querySnapshot!.documents {
                        documents.append(FirebaseDocument(documentId: doc.documentID, data: doc.data()))
                    }
                    completion(nil, documents)
                }
            }
        }
    }
    
    /**
    This function will get documents that were added after or before a specific time.  It uses paging to only retrieve a certain amount of documents at a time
     
     - parameters:
        - after: Whether to get the documents before or after a certain time
        - timeInterval: The TimeInterval to get all documents before or after
        - collection: The collection to get the documents from
        - fieldPath: The fieldPath to run the query on - **
        - limit: The number of documents to retrieve at a time
        - shouldSaveSnapshot: Wether or not to store the Firebase document snapshot if there are still more documents on Firebase that match the query
        - completion: Closure to be executed on return of objects from Firebase
     
     - Important: If there are still more documents that match the query after initial download than we store the Firestore document snapshot, however this will only work for one screen at the moment which is the ViewTagsViewController Screen,  therefore, *for any new calls to this function, **shouldSaveSnapshot should be set to false** *
     
     It's also important to remember to that the fieldPath data type needs to be a number value
     */
    func getDocumentsAdded (after:Bool, timeInterval:TimeInterval, collection:String, fieldPath:String, limit:Int, shouldSaveSnapshot:Bool = false, completion: @escaping FirebaseRequestMultiDocClosure) {
        
        let db = Firestore.firestore()
        var query:Query?
        
        // If there is a snapshot from a previous query than start our query from the latest snapshot so that we can display
        // proper paging
        if let lastDocumentSnapshot = self.lastDocumentSnapshot, shouldSaveSnapshot == true {
            if after {
                query = db.collection(collection)
                    .order(by: fieldPath, descending: true)
                    .start(afterDocument: lastDocumentSnapshot)
                    .limit(to: limit)
            } else {
                query = db.collection(collection)
                .order(by: fieldPath, descending: false)
                .start(afterDocument: lastDocumentSnapshot)
                .limit(to: limit)
            }
        } else {
            if after {
                query = db.collection(collection)
                    .order(by: fieldPath, descending: true)
                    .end(before: [timeInterval])
                    .limit(to: limit)
                    
            } else {
                query = db.collection(collection)
                    .order(by: fieldPath, descending: false)
                    .start(after: [timeInterval])
                    .limit(to: limit)
            }
        }
        
        query?.getDocuments { [weak self] (snapshot, error) in
            if let error = error {
                completion(error, nil)
            } else if let snapshot = snapshot {
                if shouldSaveSnapshot {
                    self?.lastDocumentSnapshot = snapshot.documents.last
                }
                let documents = FirebasePersistenceManager.convertSnapshotToFirebaseDocuments(querySnapshot: snapshot)
                completion(nil, documents)
            } else {
                if shouldSaveSnapshot {
                    self?.lastDocumentSnapshot = nil
                }
                completion(nil, nil)
            }
        }
    }
    
    class func getDocuments (withCollection collection: String, key: String? = nil, value: Any? = nil, queryDocument:[String:Any]? = nil, searchContainString:Bool = false, fromCache:Bool = false, completion: @escaping FirebaseRequestMultiDocClosure ) {
                
        if queryDocument != nil {
            queryWithMultipleKeyValues(withCollection: collection, queryDocument: queryDocument!, completion: completion)
        } else {
            let db = Firestore.firestore()
            var docRef:Query?
            
            if searchContainString {
                docRef = self.getSearchQuery(value: value as? String ?? "", collection: collection, key: key ?? "")
            } else {
                if let key = key {
                    docRef = db.collection(collection).whereField(key, isEqualTo: value as Any)
                } else {
                    docRef = db.collection(collection)
                }
            }
            
            docRef?.getDocuments { (querySnapshot, err) in
                if let err = err {
                    print("Error getting documents: \(err)")
                    completion(err, nil)
                } else {
                    var documents = [FirebaseDocument]()
                    guard let querySnapshot = querySnapshot else { return }
                    
                    for doc in querySnapshot.documents {
                        documents.append(FirebaseDocument(documentId: doc.documentID, data: doc.data()))
                    }
                    completion(nil, documents)
                }
            }
        }
    }
    
    class func getAndWatchCurrentUser (userId: String, completion: @escaping FirebaseRequestClosure) -> ListenerRegistration {
        let db = Firestore.firestore()
        let subscription = db.collection(kUsersCollection).document(userId).addSnapshotListener(includeMetadataChanges: true) { (document, error) in
            if let error = error {
                print(error.localizedDescription)
            }
            if let document = document {
                let firebaseDocument = self.convertDocSnapshotToFirebaseDoc(document: document)                
                completion(nil, firebaseDocument)
            }
        }
        
        return subscription        
    }
    
    class func getCurrentUser (userId: String, retryTimes:Int = 3, numberOfTimesRetried:Int = 0, completion: @escaping FirebaseRequestClosure) {
        let db = Firestore.firestore()
        
        db.collection(kUsersCollection).document(userId)
            .getDocument { (document, error) in
                if let error = error {
                    print(error.localizedDescription)
                    
                    if numberOfTimesRetried < retryTimes {
                        return FirebasePersistenceManager.getCurrentUser(userId: userId, numberOfTimesRetried: numberOfTimesRetried + 1, completion: completion)
                    } else {
                        completion(error, nil)
                    }
                }
                
                if let document = document {
                    let firebaseDocument = self.convertDocSnapshotToFirebaseDoc(document: document)
                    completion(error, firebaseDocument)
                }
        }
    }
    
    class func convertDocSnapshotToFirebaseDoc (document: DocumentSnapshot) -> FirebaseDocument {
        return FirebaseDocument(documentId: document.documentID, data: document.data() ?? [String:Any]())
    }
    
    class func getDocumentsForUser (withCollection collection: String, userId: String, completion: @escaping (Error?, [FirebaseDocument]?) -> Void) {
        let db = Firestore.firestore()
        let docRef = db.collection(collection).whereField(kUserId, isEqualTo: userId)
        docRef.getDocuments { (querySnapshot, err) in
            if let err = err {
                print("Error getting documents: \(err)")
                completion(err, nil)
            } else {
                var documents = [FirebaseDocument]()
                for doc in querySnapshot!.documents {
                    documents.append(FirebaseDocument(documentId: doc.documentID, data: doc.data()))
                }
                completion(nil, documents)
            }
        }
    }
    
    class private func getToken(result: HTTPSCallableResult) -> String? {
        if let result = result.data as? [String:Any] {
            let token = result["token"] as? String
            return token
        }
        
        return nil
    }
    
    /**
     Removes only a field from a given collection with a given document ID
     
     - parameters:
        - collection: String The Firestore collection to delete from
        - field: String The field/key to delete
        - documentID: String the document ID of the document to remove the field from
     */
    class func deleteField(collection:String, field:String, documentId:String) {
        let db = Firestore.firestore()
        db.collection(collection).document(documentId).updateData([field : FieldValue.delete()]) { (error) in
            if let _ = error {
                print("FirebasePersistenceManager.deleteField Error deleting field \(field) from collection \(collection) with document ID \(documentId)")
            } else {
                print("FirebasePersistenceManager.deleteField \(field) deleted from collection \(collection) with document ID \(documentId)")
            }
        }
    }
    
    
    /**
     Get all the documents that are near a given lat, long value
     
     - parameters:
        - latitude: Latitude to search locations nearby for
        - longitude: Longitude to search locations nearby for
        - collection: The collection to perform the search on
     
     - returns: An observable of FirebaseDocuments
     */
    func getNearbyDocumentKeys (location: CLLocation, forCollection collection:String, collectionContainingDocuments: String) -> Observable<[String]> {
        
        return Observable.create { (observer) -> Disposable in
            
            // Create the reference to our GeoFire database
            let geoFireRef = Database.database().reference(withPath: collection)
            let geoFire = GeoFire(firebaseRef: geoFireRef)
            
            // Create a query for all documents within a 250 km radius of the given location
            let center = CLLocation(latitude: location.coordinate.latitude, longitude: location.coordinate.longitude)
            let circleQuery = geoFire.query(at: center, withRadius: 250)
            
            // Keys that will be retrieved from Firebase DB
            var keys = [String]()
            
            // Get all document keys of documents retrieved from circle query
            circleQuery.observe(.keyEntered, with: { (key, location) in
                keys.append(key)
            })
            
            // Executed when all keys have been retrieved from circle query
            circleQuery.observeReady {
                observer.onNext(keys)
                observer.onCompleted()                
            }
            
            return Disposables.create()
        }
    }

    /**
     Updates one document or a group of ducments in a Firebase collection
     
     - Parameters:
        - documentId: The Document Id that should be set if only one document is being updated.  This parameter MUST be set if updating only one document.  If this parameter is set, make sure that 'documents' parameter is set to nil
        - collection: The collection to update the document on
        - updateDoc: What to update the document to if updating only one document.  This value must be set if 'documentId' is not nil
        - documents: If updating multiple documents, than use this parameter. The keys should be the documentIds updating, and the values should be what you want the document updated to.  If this parameter is set than DO NOT set the 'documentId' and 'updateDoc' parameters
        - completion: Closure that will be executed on completion of this tasks
     
     - Returns: The completion closure will contain an error if there was one, however the update document itself is not returned
     */
    class func updateDocument (withId documentId:String?, collection: String, updateDoc:[String:Any]?, documents:[String:[String:Any]]? = nil, completion: ((Error?) -> Void)?) {
        if documentId == nil && documents == nil {
            fatalError("You must either set the documents or the documentId parameter\n\nYou have two options when using this function.  You can either update just one document or multiple.  If you want to update just one, than the two required parameters are updateDoc AND documentId.  If you're updating many, than you must set a value for the documents parameter and make sure that documentId and updateDoc are nil")
        }
        
        if updateDoc == nil && documentId != nil {
            fatalError("If you set the documentId than you need to make sure that you set the updateDoc parameter with the data that you want to update.\n\nYou have two options when using this function.  You can either update just one document or multiple.  If you want to update just one, than the two required parameters are updateDoc AND documentId.  If you're updating many, than you must set a value for the documents parameter and make sure that documentId and updateDoc are nil")
        }
        
        let db = Firestore.firestore()
        let batch = db.batch()
        
        if let documents = documents {
            documents.forEach { (documentId, updateDoc) in
                let docRef = db.collection(collection).document(documentId)
                batch.updateData(updateDoc, forDocument: docRef)
            }
        } else if let documentId = documentId, let updateDoc = updateDoc {
            let docRef = db.collection(collection).document(documentId)
            batch.updateData(updateDoc, forDocument: docRef)
        }
        
        batch.commit { (err) in
            if let err = err {
                print("Error writing batch \(err)")
                if completion != nil { completion!(err) }
            } else {
                print("Batch write succeeded.")
                if completion != nil { completion!(nil) }
            }
        }
    }
    
    class func generateObject<T: Decodable>(fromFirebaseDocument document:FirebaseDocument) -> T? {
        let dict = document.data
        
        do {
            let jsonData = try JSONSerialization.data(withJSONObject: dict as Any, options: .prettyPrinted)
            let model = try JSONDecoder().decode(T.self, from: jsonData)
            return model;
        } catch {
            print(error.localizedDescription)
            return nil
        }
    }
    
    /**
     Gets an observable of a  document from Firestore with a given Id
     
     - parameters:
        - id: The Id of the document on Firestore
        - collection: The name of the collection the query for the document
     
     - returns: An observable sequence containing a single document
     */
    static func getDocumentAsObservable (withId id: String, collection:String) -> Observable<FirebaseDocument> {

        return Observable.create { (observer ) -> Disposable in
            let db = Firestore.firestore()
            let docRef = db.collection(collection).document(id)
            docRef.getDocument { (snapshot, error) in
                if let snapshot = snapshot, snapshot.exists {
                    let document = convertDocSnapshotToFirebaseDoc(document: snapshot)
                    observer.onNext(document)
                }
                
                observer.onCompleted()
            }
            
            return Disposables.create()
        }
    }
    
    static func getDocumentsAsObservable (withCollection collection:String, queryDocument:[String:Any]) -> Observable<[FirebaseDocument]> {
        
        return Observable.create { (observer) -> Disposable in
            let db = Firestore.firestore()
            let collectionRef = db.collection(collection)
            var query:Query!
            
            for key in queryDocument.keys {
                if query == nil {
                    query = collectionRef.whereField(key, isEqualTo: queryDocument[key] as Any)
               } else {
                    query = query.whereField(key, isEqualTo: queryDocument[key] as Any)
               }
            }
            
            query.getDocuments { (querySnapshot, err) in
                if let err = err {
                    observer.onError(err)
                    observer.onCompleted()
                } else {
                    if let snapshot = querySnapshot {
                        let documents = convertSnapshotToFirebaseDocuments(querySnapshot: snapshot)
                        observer.onNext(documents)
                        observer.onCompleted()
                        
                        return
                    }
                    
                    observer.onCompleted()
                }
            }
            
            return Disposables.create()
        }
        
    }
    
    class func getObjectsFromFirebaseDocuments<T: Decodable>(fromFirebaseDocuments documents:[FirebaseDocument]?) -> [T]? {
        
        var objects = [T]()
        
        documents?.forEach({ (objectDocument) in
            guard let object = FirebasePersistenceManager.generateObject(fromFirebaseDocument: objectDocument) as T? else {
                return;
            }
            
            objects.append(object)
        })
        
        return objects
    }
    
    class func arrayToFirebaseKeyValuePairs (array:[String]) -> [String:Bool] {
        var dict = [String:Bool]()
        array.forEach { (key) in
            dict[key] = true
        }
        
        return dict
    }
}

extension Array where Iterator.Element == String {
    func arrayToFirebaseKeyValuePairs () -> [String:Bool] {
        var dict = [String:Bool]()
        self.forEach { (key) in
            dict[key] = true
        }
        
        return dict
    }
}

struct FirebaseDocument {
    var documentId:String
    var data:[String: Any]
}

struct FirebaseKeyValues {
    var keys:[String]
    var values:[Any]
}

typealias FirebaseRequestClosure = (Error?, FirebaseDocument?) -> Void
typealias FirebaseRequestMultiDocClosure = (Error?, [FirebaseDocument]?) -> Void
typealias FirebaseHTTPSRequestClosure = (Error?, HTTPSCallableResult?) -> Void
