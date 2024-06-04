# import mongoose from "mongoose";

# const followingMangaSchema = mongoose.Schema(
#   {
#     mangaId: {
#       type: mongoose.Schema.Types.ObjectId,
#       ref: "Manga",
#       require: true,
#     },
#     readChapter: {
#       type: mongoose.Schema.Types.ObjectId,
#       ref: "Manga.Chapter",
#       default: null,
#     },
#     readedChapter: [
#       {
#         type: mongoose.Schema.Types.ObjectId,
#         ref: "Manga.Chapter",
#       },
#     ],
#   },
#   { timestamps: true }
# );

# const UserSchema = mongoose.Schema(
#   {
#     fullName: {
#       type: String,
#       required: [true, "Làm ơn nhập họ tên"],
#     },
#     email: {
#       type: String,
#       required: [true, "Làm ơn nhập email"],
#       unique: true,
#       trim: true,
#     },
#     password: {
#       type: String,
#       required: [true, "Làm ơn nhập mật khẩu"],
#       minlength: [8, "Mật khẩu chứ ít nhất 8 ký tự"],
#       validate: {
#         validator: function (value) {
#           // Regular expression to check for at least one number and one character
#           return /^(?=.*\d)(?=.*[a-zA-Z]).{8,}$/.test(value);
#         },
#         message: "Mật khẩu phải chứa cả kí tự lẫn số",
#       },
#     },
#     image: {
#       type: String,
#       default: null,
#     },
#     isAdmin: {
#       type: Boolean,
#       default: false,
#     },
#     followingManga: [followingMangaSchema],
#     isVerified: {
#       type: Boolean,
#       required: true,
#       default: false,
#     },
#     numberOfWarnings: { type: Number, require: true, default: 0 },
#     enable: { type: Number, require: true, default: 1 },
#     recommendList: [
#       {
#         type: mongoose.Schema.Types.ObjectId,
#         ref: "Manga",
#       },
#     ],
#   },
#   {
#     timestamps: true,
#   }
# );

# export default mongoose.model("User", UserSchema);
