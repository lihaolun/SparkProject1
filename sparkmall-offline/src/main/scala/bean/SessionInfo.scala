package bean

case class SessionInfo(taskId: String, sessionId: String, startTime: String,
                       stepLenth: Long, visitLenth: Long, searchKeyWords: String,
                       clickProductIds: String, orderProductIds: String, payProductIds: String) {
}
